"""
Durable state for the tracker.

Every judgment the tracker knows about is one row in `items`, moving through:

    discovered -> downloading -> downloaded -> uploading -> pushed
                                     |             |
                                     +-> failed <--+           (retryable)
                                     +-> skipped               (terminal, no retry)

The database is the single source of truth. Nothing is held only in memory, so
killing the process mid-batch loses at most the bytes of the files currently in
flight — those rows revert to `discovered` on the next start.

Disk usage is derived from `bytes` summed over rows that still have a local
file (`downloaded`/`uploading`), never from a counter that could drift.
"""

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

from config import DB_PATH

#: States whose rows own a file on local disk and therefore consume quota.
#: Failed uploads keep their local_path for retry, so they hold quota too.
ON_DISK_STATES = ("downloaded", "uploading", "failed")

#: States that consume staging capacity. `downloading` has no complete file yet,
#: but its bytes are already promised — counting them is what stops two claims
#: from spending the same free space twice.
RESERVED_STATES = ON_DISK_STATES + ("downloading",)

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT NOT NULL,              -- 'hc' | 'sc'
    s3_key          TEXT NOT NULL UNIQUE,       -- dedupe key across all runs
    bucket          TEXT NOT NULL,
    bytes           INTEGER NOT NULL DEFAULT 0, -- size on S3 (== size on disk)
    citation        TEXT,                       -- ingestion citation, derived
                                                -- from s3_key: unique by
                                                -- construction
    source_citation TEXT,                       -- citation as reported by the
                                                -- source metadata (not unique)
    title           TEXT,
    year            INTEGER,
    court           TEXT,
    state_name      TEXT,
    bench           TEXT,
    case_type       TEXT,
    judges          TEXT,
    parties         TEXT,
    outcome         TEXT,
    language        TEXT DEFAULT 'en',
    source_url      TEXT,
    cnr             TEXT,
    status          TEXT NOT NULL DEFAULT 'discovered',
    local_path      TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    error           TEXT,
    document_id     INTEGER,                    -- id returned by the corpus API
    batch_id        INTEGER,
    discovered_at   REAL NOT NULL,
    downloaded_at   REAL,
    pushed_at       REAL
);

CREATE INDEX IF NOT EXISTS idx_items_status  ON items(status);
CREATE INDEX IF NOT EXISTS idx_items_batch   ON items(batch_id);
CREATE INDEX IF NOT EXISTS idx_items_source  ON items(source);
CREATE UNIQUE INDEX IF NOT EXISTS idx_items_citation
    ON items(citation) WHERE citation IS NOT NULL;

CREATE TABLE IF NOT EXISTS batches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    opened_at    REAL NOT NULL,
    closed_at    REAL,
    pushed_at    REAL,
    status       TEXT NOT NULL DEFAULT 'open'   -- open|full|pushing|done
);

-- Which (source, year, court, bench) partitions have already been enumerated,
-- so a restart does not re-list S3 from scratch.
CREATE TABLE IF NOT EXISTS scanned_partitions (
    prefix          TEXT PRIMARY KEY,
    source          TEXT NOT NULL,
    found           INTEGER NOT NULL DEFAULT 0,
    queued          INTEGER NOT NULL DEFAULT 0,
    scanned_at      REAL NOT NULL,
    -- pending: never completed. done: enumerated. failed: retries exhausted.
    status          TEXT NOT NULL DEFAULT 'done',
    attempts        INTEGER NOT NULL DEFAULT 0,
    error           TEXT,
    next_attempt_at REAL NOT NULL DEFAULT 0
);

-- Free-form key/value for worker control flags and cursors.
CREATE TABLE IF NOT EXISTS meta (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    at         REAL NOT NULL,
    level      TEXT NOT NULL,      -- info|warn|error
    message    TEXT NOT NULL,
    item_id    INTEGER
);

CREATE INDEX IF NOT EXISTS idx_events_at ON events(at DESC);
"""

_local = threading.local()
_write_lock = threading.Lock()


def connect() -> sqlite3.Connection:
    """One connection per thread; WAL so the API can read while the worker writes."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        _local.conn = conn
    return conn


@contextmanager
def write() -> Iterator[sqlite3.Connection]:
    """Serialised write transaction. SQLite allows one writer; make that explicit."""
    conn = connect()
    with _write_lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except Exception:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")


#: Columns added after the first release. `CREATE TABLE IF NOT EXISTS` does not
#: touch an existing table, so an older state.db needs them added explicitly.
_MIGRATIONS = {
    "items": [
        ("source_citation", "TEXT"),
    ],
    "scanned_partitions": [
        ("status", "TEXT NOT NULL DEFAULT 'done'"),
        ("attempts", "INTEGER NOT NULL DEFAULT 0"),
        ("error", "TEXT"),
        ("next_attempt_at", "REAL NOT NULL DEFAULT 0"),
    ],
}


def _migrate(conn: sqlite3.Connection) -> None:
    for table, columns in _MIGRATIONS.items():
        existing = {
            r["name"] for r in conn.execute(f"PRAGMA table_info({table})")
        }
        if not existing:  # table did not exist; SCHEMA just created it in full
            continue
        for name, decl in columns:
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def init() -> None:
    conn = connect()
    with _write_lock:
        conn.executescript(SCHEMA)
        _migrate(conn)
    # Anything left mid-flight by a hard kill is not trustworthy — rewind it.
    recovered = reset_inflight()
    orphans = reconcile_staging()
    if recovered:
        log("warn", f"Recovered {recovered} in-flight item(s) after restart")
    if orphans:
        log("warn", f"Found {orphans} staging files unclaimed by DB — review manually")


def reconcile_staging() -> int:
    """
    On startup, find staging files whose DB row is gone or no longer claims them.

    Does NOT auto-delete them — operator reviews via dashboard. Returns count found.
    """
    from config import STAGING_DIR

    claimed = {
        r["local_path"]
        for r in connect().execute("SELECT local_path FROM items WHERE local_path IS NOT NULL")
    }
    orphan_count = 0
    for path in STAGING_DIR.rglob("*.pdf"):
        if str(path) not in claimed:
            orphan_count += 1
    return orphan_count


def reset_inflight() -> int:
    """
    Rewind rows stuck in a transient state by an unclean shutdown, checking each
    against the filesystem rather than assuming.

      downloading -> discovered      (partial file is deleted; nothing to keep)
      uploading   -> downloaded      if the staged file is still there
                  -> discovered      if it is not, so it is fetched again

    An `uploading` row may in fact have reached the backend before the crash. It
    is re-uploaded on restart; the corpus answers 409 for a true duplicate and
    the item lands as `skipped`, which is the deterministic outcome.
    """
    n = 0
    with write() as conn:
        rows = conn.execute(
            "SELECT id, local_path FROM items WHERE status IN ('downloading','uploading')"
        ).fetchall()
        for row in rows:
            path = Path(row["local_path"]) if row["local_path"] else None
            keep = path is not None and path.exists()
            if keep:
                conn.execute(
                    "UPDATE items SET status='downloaded' WHERE id=?", (row["id"],)
                )
            else:
                conn.execute(
                    "UPDATE items SET status='discovered', local_path=NULL WHERE id=?",
                    (row["id"],),
                )
            n += 1
    _sweep_partials()
    return n


def _sweep_partials() -> None:
    """Delete `.part` files: a partial download is never resumable here."""
    from config import STAGING_DIR

    for path in STAGING_DIR.rglob("*.part"):
        try:
            path.unlink()
        except OSError:
            pass


# ------------------------------------------------------------------------ meta


def get_meta(key: str, default: Any = None) -> Any:
    row = connect().execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    if row is None:
        return default
    try:
        return json.loads(row["value"])
    except json.JSONDecodeError:
        return row["value"]


def set_meta(key: str, value: Any) -> None:
    with write() as conn:
        conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )


# ---------------------------------------------------------------------- events


def log(level: str, message: str, item_id: Optional[int] = None) -> None:
    with write() as conn:
        conn.execute(
            "INSERT INTO events(at,level,message,item_id) VALUES(?,?,?,?)",
            (time.time(), level, message, item_id),
        )
        # Keep the log bounded; it is a live feed, not an archive.
        conn.execute(
            "DELETE FROM events WHERE id < (SELECT MAX(id)-2000 FROM events)"
        )


def recent_events(limit: int = 100) -> list[dict]:
    rows = connect().execute(
        "SELECT at, level, message, item_id FROM events ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


# --------------------------------------------------------------------- batches


def open_batch() -> int:
    """Return the current open batch, creating one if needed."""
    row = connect().execute(
        "SELECT id FROM batches WHERE status='open' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row:
        return int(row["id"])
    with write() as conn:
        cur = conn.execute(
            "INSERT INTO batches(opened_at,status) VALUES(?, 'open')", (time.time(),)
        )
        return int(cur.lastrowid)


def set_batch_status(batch_id: int, status: str) -> None:
    stamp_col = {"full": "closed_at", "done": "pushed_at"}.get(status)
    with write() as conn:
        if stamp_col:
            conn.execute(
                f"UPDATE batches SET status=?, {stamp_col}=? WHERE id=?",
                (status, time.time(), batch_id),
            )
        else:
            conn.execute("UPDATE batches SET status=? WHERE id=?", (status, batch_id))


# ----------------------------------------------------------------------- items


def add_items(rows: list[dict]) -> int:
    """
    Insert discovered items, ignoring any whose s3_key or citation already
    exists. Returns the number actually inserted.

    Dedupe happens here rather than downstream so a re-scan of the same
    partition is free and idempotent.
    """
    if not rows:
        return 0
    inserted = 0
    now = time.time()
    with write() as conn:
        for r in rows:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO items(
                    source, s3_key, bucket, bytes, citation, source_citation, title, year, court,
                    state_name, bench, case_type, judges, parties, outcome,
                    language, source_url, cnr, status, discovered_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'discovered', ?)
                """,
                (
                    r["source"], r["s3_key"], r["bucket"], r.get("bytes", 0),
                    r.get("citation"), r.get("source_citation"), r.get("title"), r.get("year"), r.get("court"),
                    r.get("state_name"), r.get("bench"), r.get("case_type"),
                    r.get("judges"), r.get("parties"), r.get("outcome"),
                    r.get("language", "en"), r.get("source_url"), r.get("cnr"), now,
                ),
            )
            inserted += cur.rowcount or 0
    return inserted


def bytes_on_disk() -> int:
    """
    Live disk usage, derived from item state rather than a running counter.

    NOTE: This only counts files the DB knows about. Use `filesystem_bytes()`
    for actual staging directory usage including orphans.
    """
    row = connect().execute(
        f"SELECT COALESCE(SUM(bytes),0) AS b FROM items "
        f"WHERE status IN {ON_DISK_STATES}"
    ).fetchone()
    return int(row["b"])


def reserved_bytes(conn: Optional[sqlite3.Connection] = None) -> int:
    """
    Bytes already spoken for: complete files plus in-flight downloads.

    This is the number the disk cap is enforced against. Taking an optional
    connection lets `claim_for_download` read it inside its own transaction.
    """
    conn = conn or connect()
    row = conn.execute(
        f"SELECT COALESCE(SUM(bytes),0) AS b FROM items "
        f"WHERE status IN {RESERVED_STATES}"
    ).fetchone()
    return int(row["b"])


def filesystem_bytes() -> int:
    """Actual disk usage from the staging directory, including orphans."""
    from config import STAGING_DIR
    total = 0
    for path in STAGING_DIR.rglob("*.pdf"):
        try:
            total += path.stat().st_size
        except OSError:
            pass
    return total


def claim_for_download(cap_bytes: int, limit: int = 8) -> list[dict]:
    """
    Atomically claim up to `limit` discovered items whose combined size fits
    under `cap_bytes`, flipping them to `downloading`.

    The budget is recomputed from `RESERVED_STATES` *inside* the write
    transaction, so concurrent claims cannot both spend the same free space —
    in-flight `downloading` rows are already charged against the cap.
    """
    claimed: list[dict] = []
    with write() as conn:
        budget = cap_bytes - reserved_bytes(conn)
        if budget <= 0:
            return []
        rows = conn.execute(
            "SELECT * FROM items WHERE status='discovered' "
            "AND bytes <= ? ORDER BY id LIMIT ?",
            (budget, limit * 4),
        ).fetchall()
        spent = 0
        for row in rows:
            size = int(row["bytes"])
            if spent + size > budget:
                continue
            conn.execute(
                "UPDATE items SET status='downloading' WHERE id=? AND status='discovered'",
                (row["id"],),
            )
            spent += size
            claimed.append(dict(row))
            if len(claimed) >= limit:
                break
    return claimed


def mark_downloaded(item_id: int, local_path: str, actual_bytes: int, batch_id: int) -> None:
    with write() as conn:
        conn.execute(
            "UPDATE items SET status='downloaded', local_path=?, bytes=?, "
            "downloaded_at=?, batch_id=?, error=NULL WHERE id=?",
            (local_path, actual_bytes, time.time(), batch_id, item_id),
        )


def claim_for_upload(limit: int = 4) -> list[dict]:
    with write() as conn:
        rows = conn.execute(
            "SELECT * FROM items WHERE status='downloaded' ORDER BY id LIMIT ?",
            (limit,),
        ).fetchall()
        for row in rows:
            conn.execute("UPDATE items SET status='uploading' WHERE id=?", (row["id"],))
        return [dict(r) for r in rows]


def mark_pushed(item_id: int, document_id: Optional[int]) -> None:
    """Push succeeded — the local file is gone, so this row no longer holds quota."""
    with write() as conn:
        conn.execute(
            "UPDATE items SET status='pushed', document_id=?, pushed_at=?, "
            "local_path=NULL, error=NULL WHERE id=?",
            (document_id, time.time(), item_id),
        )


def mark_skipped(item_id: int, reason: str) -> None:
    """
    Terminal, non-retryable. Used for duplicates the corpus already has and for
    objects that can never be ingested (oversized, unreadable).
    """
    with write() as conn:
        conn.execute(
            "UPDATE items SET status='skipped', error=?, local_path=NULL WHERE id=?",
            (reason[:500], item_id),
        )


def mark_failed(item_id: int, error: str, retry_state: str) -> str:
    """
    Record a failure. Below MAX_ATTEMPTS the row returns to `retry_state` for
    another go; at the limit it parks as `failed` for manual retry.

    Returns the state the row ended in.
    """
    from config import MAX_ATTEMPTS

    with write() as conn:
        row = conn.execute("SELECT attempts FROM items WHERE id=?", (item_id,)).fetchone()
        attempts = int(row["attempts"]) + 1 if row else 1
        final = attempts >= MAX_ATTEMPTS
        status = "failed" if final else retry_state
        conn.execute(
            "UPDATE items SET status=?, attempts=?, error=? WHERE id=?",
            (status, attempts, error[:500], item_id),
        )
        return status


def retry_failed() -> int:
    """
    Requeue everything parked as `failed`, resetting the attempt counter.

    A row that still has its file on disk goes back to `downloaded` (upload
    stage); one without goes back to `discovered` (download stage).
    """
    with write() as conn:
        cur = conn.execute(
            """
            UPDATE items SET
                status = CASE
                    WHEN local_path IS NOT NULL THEN 'downloaded'
                    ELSE 'discovered' END,
                attempts = 0,
                error = NULL
            WHERE status='failed'
            """
        )
        return cur.rowcount or 0


def stats() -> dict:
    conn = connect()
    counts = {
        r["status"]: int(r["n"])
        for r in conn.execute("SELECT status, COUNT(*) n FROM items GROUP BY status")
    }
    by_source = [
        dict(r)
        for r in conn.execute(
            f"""
            SELECT source,
                   COUNT(*)                                          AS total,
                   SUM(status='pushed')                              AS pushed,
                   SUM(status='discovered')                          AS queued,
                   SUM(status IN {ON_DISK_STATES})                   AS on_disk,
                   SUM(status='failed')                              AS failed,
                   SUM(status='skipped')                             AS skipped,
                   COALESCE(SUM(CASE WHEN status='pushed' THEN bytes END),0) AS pushed_bytes
            FROM items GROUP BY source
            """
        )
    ]
    totals = conn.execute(
        """
        SELECT COUNT(*) AS total,
               COALESCE(SUM(CASE WHEN status='pushed' THEN bytes END),0) AS pushed_bytes,
               COALESCE(SUM(bytes),0)                                   AS known_bytes
        FROM items
        """
    ).fetchone()
    return {
        "counts": counts,
        "by_source": by_source,
        "total_items": int(totals["total"]),
        "pushed_bytes": int(totals["pushed_bytes"]),
        "known_bytes": int(totals["known_bytes"]),
        "bytes_on_disk": bytes_on_disk(),
        "filesystem_bytes": filesystem_bytes(),
    }


def list_items(status: Optional[str] = None, limit: int = 50, offset: int = 0) -> list[dict]:
    sql = (
        "SELECT id, source, s3_key, bytes, citation, title, year, court, bench, "
        "status, attempts, error, document_id, batch_id FROM items"
    )
    # A status filter is validated against the closed set by the API layer.
    params: list[Any] = []
    if status:
        sql += " WHERE status=?"
        params.append(status)
    sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params += [limit, offset]
    return [dict(r) for r in connect().execute(sql, params).fetchall()]


def stale_local_files() -> list[Path]:
    """
    Files in staging that no live row claims. Produced when a push succeeds but
    the unlink fails, or a row is deleted by hand.

    Any row with a local_path (including failed retryable uploads) is protected.
    """
    from config import STAGING_DIR

    claimed = {
        r["local_path"]
        for r in connect().execute(
            "SELECT local_path FROM items WHERE local_path IS NOT NULL"
        )
    }
    orphans = []
    for path in STAGING_DIR.rglob("*.pdf"):
        if str(path) not in claimed:
            orphans.append(path)
    return orphans


def mark_partition_scanned(prefix: str, source: str, found: int, queued: int, status: str = "done") -> None:
    with write() as conn:
        conn.execute(
            "INSERT INTO scanned_partitions(prefix,source,found,queued,scanned_at,status,attempts) "
            "VALUES(?,?,?,?,?,?,0) ON CONFLICT(prefix) DO UPDATE SET "
            "found=excluded.found, queued=excluded.queued, scanned_at=excluded.scanned_at, "
            "status=excluded.status, attempts=0, error=NULL",
            (prefix, source, found, queued, time.time(), status),
        )


def is_partition_scanned(prefix: str) -> bool:
    """Check if partition was successfully scanned (status='done')."""
    return (
        connect()
        .execute("SELECT 1 FROM scanned_partitions WHERE prefix=? AND status='done'", (prefix,))
        .fetchone()
        is not None
    )


def scanned_partitions() -> list[dict]:
    return [
        dict(r)
        for r in connect().execute(
            "SELECT prefix, source, found, queued, scanned_at, status, attempts, error "
            "FROM scanned_partitions ORDER BY scanned_at DESC LIMIT 200"
        )
    ]


def mark_partition_attempt_failed(prefix: str, source: str, error: str) -> str:
    """
    Record a failed scan attempt. Stays `pending` (retryable, with backoff)
    until MAX_SCAN_ATTEMPTS, then parks as `failed`.

    Returns the status the partition ended in.
    """
    from config import MAX_SCAN_ATTEMPTS

    with write() as conn:
        row = conn.execute(
            "SELECT attempts FROM scanned_partitions WHERE prefix=?", (prefix,)
        ).fetchone()
        attempts = int(row["attempts"]) + 1 if row else 1
        status = "failed" if attempts >= MAX_SCAN_ATTEMPTS else "pending"
        # Exponential backoff, capped, so a transient S3 outage is not hammered.
        delay = min(60 * (2 ** (attempts - 1)), 900)
        conn.execute(
            "INSERT INTO scanned_partitions"
            "(prefix,source,found,queued,scanned_at,status,attempts,error,next_attempt_at) "
            "VALUES(?,?,0,0,?,?,?,?,?) ON CONFLICT(prefix) DO UPDATE SET "
            "source=excluded.source, scanned_at=excluded.scanned_at, "
            "status=excluded.status, attempts=excluded.attempts, "
            "error=excluded.error, next_attempt_at=excluded.next_attempt_at",
            (prefix, source, time.time(), status, attempts, error[:500],
             time.time() + delay),
        )
        return status


def due_partitions(limit: int = 1) -> list[dict]:
    """Pending partitions whose backoff has expired, oldest first."""
    return [
        dict(r)
        for r in connect().execute(
            "SELECT prefix, source, attempts FROM scanned_partitions "
            "WHERE status='pending' AND next_attempt_at <= ? "
            "ORDER BY next_attempt_at LIMIT ?",
            (time.time(), limit),
        )
    ]


def partition_backlog() -> dict:
    """Counts of partitions that are not successfully scanned."""
    row = connect().execute(
        "SELECT COALESCE(SUM(status='pending'),0) AS pending, "
        "       COALESCE(SUM(status='failed'),0)  AS failed "
        "FROM scanned_partitions"
    ).fetchone()
    return {"pending": int(row["pending"]), "failed": int(row["failed"])}


def retry_failed_partitions() -> int:
    """Return every parked partition to the retry queue."""
    with write() as conn:
        cur = conn.execute(
            "UPDATE scanned_partitions SET status='pending', attempts=0, "
            "error=NULL, next_attempt_at=0 WHERE status='failed'"
        )
        return cur.rowcount or 0
