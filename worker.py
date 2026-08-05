"""
The download/push engine.

One background thread runs a loop with three jobs, in priority order:

  1. push   — upload staged PDFs to the corpus API and delete them locally,
              which is what frees quota
  2. download — claim discovered items that fit in the remaining quota and
              fetch them from S3
  3. scan   — enumerate more S3 partitions when the queue runs dry

The 1 GiB cap is enforced in `state.claim_for_download`, inside the same write
transaction that flips items to `downloading`. Concurrent download threads
therefore cannot collectively overshoot: the budget is spent atomically before
any byte is fetched.

Pause is cooperative and immediate at task granularity — in-flight downloads
finish (a few hundred KB each), nothing new starts, and every completed unit is
already committed to SQLite, so resume needs no reconstruction.

Worker process lock: only one worker may run at a time. The lock lives at
STAGING_DIR/.worker.lock and is held by the process, released on stop or crash.
"""

from __future__ import annotations

import fcntl
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Optional

import state
from config import (
    AUTO_PUSH,
    DISK_CAP_BYTES,
    DOWNLOAD_WORKERS,
    STAGING_DIR,
    UPLOAD_WORKERS,
)
from pipeline import PushAuthError, PushError, PushRejected, client
from sources import (
    discover_hc,
    discover_sc,
    download_object,
    expand_targets,
    partition_prefix,
)

#: Meta keys holding the control flags, so they survive a restart.
K_PAUSED = "paused"
K_PLAN = "scan_plan"
K_PLAN_POS = "scan_plan_pos"
K_AUTO_PUSH = "auto_push"

_LOCK_PATH = STAGING_DIR / ".worker.lock"


class WorkerLockError(Exception):
    """Another worker already running."""


class Worker:
    """Single background loop. Start once; control it with pause/resume."""

    def __init__(self) -> None:
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._wake = threading.Event()
        self.phase = "idle"
        self.current: dict[str, Any] = {}
        self._lock = threading.Lock()
        self._lock_fd: Optional[int] = None

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._acquire_lock()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="tracker-worker", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=30)
        self._release_lock()

    def _acquire_lock(self) -> None:
        """Grab OS-level file lock. Raises WorkerLockError if another process holds it."""
        _LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(_LOCK_PATH, os.O_CREAT | os.O_WRONLY, 0o644)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._lock_fd = fd
        except (OSError, BlockingIOError) as exc:
            raise WorkerLockError(
                "Another worker is already running. Stop it or wait for it to finish."
            ) from exc

    def _release_lock(self) -> None:
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
                os.close(self._lock_fd)
            except OSError:
                pass
            self._lock_fd = None

    def nudge(self) -> None:
        """Wake the loop early — after a plan change, retry, or resume."""
        self._wake.set()

    # --------------------------------------------------------------- control

    @property
    def paused(self) -> bool:
        return bool(state.get_meta(K_PAUSED, False))

    def pause(self, reason: str = "manual") -> None:
        state.set_meta(K_PAUSED, True)
        state.log("info", f"Paused ({reason})")
        self._wake.set()

    def resume(self) -> None:
        state.set_meta(K_PAUSED, False)
        state.log("info", "Resumed")
        self._wake.set()

    @property
    def auto_push(self) -> bool:
        return bool(state.get_meta(K_AUTO_PUSH, AUTO_PUSH))

    def set_auto_push(self, enabled: bool) -> None:
        state.set_meta(K_AUTO_PUSH, bool(enabled))
        state.log("info", f"Auto-push {'enabled' if enabled else 'disabled'}")
        self._wake.set()

    # ------------------------------------------------------------------ plan

    def set_plan(self, targets: list[dict], replace: bool = True) -> int:
        """
        Install the list of S3 partitions to enumerate.

        Partitions already scanned in a previous run are dropped, so re-planning
        the same range is cheap and does not re-list S3.
        """
        fresh = [
            t for t in targets
            if not state.is_partition_scanned(
                partition_prefix(t["source"], t["year"], t.get("court"), t.get("bench"))
            )
        ]
        if replace:
            state.set_meta(K_PLAN, fresh)
            state.set_meta(K_PLAN_POS, 0)
        else:
            existing = state.get_meta(K_PLAN, []) or []
            seen = {
                partition_prefix(t["source"], t["year"], t.get("court"), t.get("bench"))
                for t in existing
            }
            merged = existing + [
                t for t in fresh
                if partition_prefix(t["source"], t["year"], t.get("court"), t.get("bench"))
                not in seen
            ]
            state.set_meta(K_PLAN, merged)
        self._wake.set()
        return len(fresh)

    def plan_progress(self) -> dict:
        plan = state.get_meta(K_PLAN, []) or []
        pos = int(state.get_meta(K_PLAN_POS, 0) or 0)
        return {"total": len(plan), "position": min(pos, len(plan)), "remaining": max(0, len(plan) - pos)}

    # -------------------------------------------------------------- main loop

    def _run(self) -> None:
        state.log("info", "Worker started")
        while not self._stop.is_set():
            try:
                did_work = self._tick()
            except Exception as exc:  # a bug here must not kill the loop
                state.log("error", f"Worker tick failed: {exc}")
                did_work = False
            # Busy when there is work; otherwise wait to be nudged.
            self._wake.wait(timeout=0.5 if did_work else 5.0)
            self._wake.clear()
        self.phase = "stopped"
        state.log("info", "Worker stopped")

    def _tick(self) -> bool:
        if self.paused:
            self.phase = "paused"
            return False

        # 1. Push first — it is the only thing that frees quota.
        if self.auto_push and self._pending_uploads():
            return self._do_push()

        used = state.reserved_bytes()
        budget = DISK_CAP_BYTES - used

        # The cap binds when the smallest queued item no longer fits — not only
        # at exactly zero free bytes. Reporting "idle" in that state would hide
        # the fact that work is waiting on the cap.
        smallest = self._smallest_queued()
        cap_bound = smallest is not None and smallest > budget

        if cap_bound:
            batch = state.open_batch()
            if self._pending_uploads():
                # Auto-push would have run above, so reaching here means it is
                # off and the batch is waiting for a manual push.
                if state.get_meta("cap_notified") != batch:
                    state.set_meta("cap_notified", batch)
                    state.set_batch_status(batch, "full")
                    state.log(
                        "warn",
                        f"Disk cap reached ({used / 1048576:.0f} MiB staged, "
                        f"{budget / 1048576:.0f} MiB free). Auto-push is off — "
                        "press \"Push now\" to clear the batch and continue.",
                    )
                self.phase = "cap-reached"
            else:
                # Nothing left to push, yet nothing fits: every staged item is
                # parked as failed, so the cap cannot clear on its own.
                self.phase = "cap-blocked"
                if state.get_meta("cap_blocked_notified") != batch:
                    state.set_meta("cap_blocked_notified", batch)
                    state.log(
                        "error",
                        f"Cap held by {used / 1048576:.0f} MiB of failed items with "
                        "nothing pushable — retry them or clean up to continue.",
                    )
            return False

        # 2. Download whatever fits. The cap itself is passed down; the real
        # budget is recomputed inside the claim transaction.
        if self._download_round():
            return True

        # 3. Queue empty — enumerate more S3 partitions.
        return self._scan_round()

    @staticmethod
    def _smallest_queued() -> Optional[int]:
        """Size of the smallest item awaiting download, or None if none await."""
        row = state.connect().execute(
            "SELECT MIN(bytes) AS b FROM items WHERE status='discovered'"
        ).fetchone()
        return None if row is None or row["b"] is None else int(row["b"])

    # ------------------------------------------------------------- downloads

    def _download_round(self) -> bool:
        claimed = state.claim_for_download(DISK_CAP_BYTES, limit=DOWNLOAD_WORKERS * 2)
        if not claimed:
            return False

        batch = state.open_batch()
        self.phase = "downloading"
        with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
            futures = {pool.submit(self._download_one, item, batch): item for item in claimed}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    status = state.mark_failed(item["id"], str(exc), "discovered")
                    level = "error" if status == "failed" else "warn"
                    state.log(level, f"Download failed: {item['s3_key']} — {exc}", item["id"])
        return True

    def _download_one(self, item: dict, batch_id: int) -> None:
        # Use item ID in path to guarantee uniqueness — basename alone can collide
        # across different partitions. Pattern: batch{N}/{source}/{item_id}_{basename}
        basename = Path(item["s3_key"]).name
        dest = STAGING_DIR / f"batch{batch_id}" / item["source"] / f"{item['id']}_{basename}"

        with self._lock:
            self.current = {"stage": "download", "key": item["s3_key"]}

        size = download_object(item["bucket"], item["s3_key"], dest)
        state.mark_downloaded(item["id"], str(dest), size, batch_id)

    # ----------------------------------------------------------------- pushes

    def _pending_uploads(self) -> bool:
        row = state.connect().execute(
            "SELECT 1 FROM items WHERE status='downloaded' LIMIT 1"
        ).fetchone()
        return row is not None

    def _do_push(self) -> bool:
        claimed = state.claim_for_upload(limit=UPLOAD_WORKERS * 2)
        if not claimed:
            return False

        batch_id = claimed[0].get("batch_id") if claimed else None
        self.phase = "pushing"
        with ThreadPoolExecutor(max_workers=UPLOAD_WORKERS) as pool:
            futures = {pool.submit(self._push_one, item): item for item in claimed}
            for future in as_completed(futures):
                item = futures[future]
                try:
                    future.result()
                except PushAuthError as exc:
                    # Every other upload will fail the same way. Stop rather
                    # than burn through the queue marking everything failed.
                    state.mark_failed(item["id"], str(exc), "downloaded")
                    self.pause(f"auth error: {exc}")
                    state.log("error", f"Push halted — {exc}", item["id"])
                except Exception as exc:
                    status = state.mark_failed(item["id"], str(exc), "downloaded")
                    level = "error" if status == "failed" else "warn"
                    state.log(level, f"Push failed: {item.get('citation')} — {exc}", item["id"])

        # After push pass, check if the batch is done and mark it.
        if batch_id:
            self._maybe_close_batch(batch_id)
        return True

    @staticmethod
    def _maybe_close_batch(batch_id: int) -> None:
        """Mark batch done if all its items are terminal."""
        row = state.connect().execute(
            "SELECT COUNT(*) AS total, "
            "SUM(status IN ('pushed','skipped','failed')) AS terminal "
            "FROM items WHERE batch_id=?",
            (batch_id,),
        ).fetchone()
        if row and row["total"] > 0 and row["total"] == row["terminal"]:
            state.set_batch_status(batch_id, "done")
            state.log("info", f"Batch {batch_id} complete")

    def _push_one(self, item: dict) -> None:
        path_str = item.get("local_path")
        if not path_str:
            raise PushError("Item has no local file recorded")
        path = Path(path_str)

        # Verify file still exists before attempting upload. If it was deleted
        # (orphan cleanup run incorrectly, manual removal), reset to download
        # stage rather than failing permanently.
        if not path.exists():
            state.log("warn", f"Local file vanished before upload: {path}. Re-downloading.", item["id"])
            with state.write() as conn:
                conn.execute(
                    "UPDATE items SET status='discovered', local_path=NULL WHERE id=?",
                    (item["id"],),
                )
            return

        with self._lock:
            self.current = {"stage": "push", "citation": item.get("citation")}

        try:
            doc_id = client.push(item, path)
        except PushRejected as exc:
            # Permanent: drop the file so the quota is released, and do not retry.
            self._discard(path)
            state.mark_skipped(item["id"], str(exc))
            state.log("info", f"Skipped {item.get('citation')}: {exc}", item["id"])
            return

        self._discard(path)
        state.mark_pushed(item["id"], doc_id)

    @staticmethod
    def _discard(path: Path) -> None:
        """
        Delete a staged PDF. Called only after the backend has taken ownership
        (201) or permanently refused it — never while an upload could still be
        retried.
        """
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            state.log("warn", f"Could not delete {path}: {exc}")
        # Prune the now-possibly-empty batch directory.
        try:
            parent = path.parent
            if parent.is_dir() and not any(parent.iterdir()):
                parent.rmdir()
        except OSError:
            pass

    # ------------------------------------------------------------------ scans

    def _scan_round(self) -> bool:
        plan = state.get_meta(K_PLAN, []) or []
        pos = int(state.get_meta(K_PLAN_POS, 0) or 0)
        if pos >= len(plan):
            # The plan is walked, but partitions whose scan failed are still
            # owed a retry. Reporting `idle` here would hide missing years.
            if self._retry_scan_round():
                return True
            backlog = state.partition_backlog()
            self.phase = "scan-incomplete" if backlog["failed"] else "idle"
            return False

        target = plan[pos]
        self.phase = "scanning"
        prefix = partition_prefix(
            target["source"], target["year"], target.get("court"), target.get("bench")
        )
        with self._lock:
            self.current = {"stage": "scan", "partition": prefix}

        # Advance the cursor first: the partition's own row now owns its
        # retry state, so a failure is never lost by the cursor moving on.
        state.set_meta(K_PLAN_POS, pos + 1)
        if not self._scan_partition(target, prefix):
            return True
        return True

    def _scan_partition(self, target: dict, prefix: str) -> bool:
        """Enumerate one partition. Returns True on success."""
        try:
            if target["source"] == "hc":
                items, seen, meta_error = discover_hc(
                    target["year"], target["court"], target["bench"]
                )
            else:
                items, seen, meta_error = discover_sc(
                    target["year"], target.get("court") or "english"
                )

            if meta_error:
                state.log(
                    "warn",
                    f"{prefix}: metadata read failed ({meta_error}), ingesting PDFs without it",
                )
        except Exception as exc:
            status = state.mark_partition_attempt_failed(prefix, target["source"], str(exc))
            level = "error" if status == "failed" else "warn"
            state.log(
                level,
                f"Scan failed for {prefix}: {exc}"
                + ("" if status == "failed" else " — will retry"),
            )
            return False

        queued = state.add_items(items)
        state.mark_partition_scanned(prefix, target["source"], seen, queued)
        state.log(
            "info",
            f"Scanned {prefix}: {seen} objects, {len(items)} usable, {queued} newly queued",
        )
        return True

    def _retry_scan_round(self) -> bool:
        """Re-attempt one partition whose backoff has expired."""
        due = state.due_partitions(limit=1)
        if not due:
            return False
        row = due[0]
        target = self._target_from_prefix(row["prefix"], row["source"])
        if target is None:
            state.mark_partition_attempt_failed(
                row["prefix"], row["source"], "Unparseable partition prefix"
            )
            return True
        self.phase = "scanning"
        with self._lock:
            self.current = {"stage": "scan-retry", "partition": row["prefix"]}
        self._scan_partition(target, row["prefix"])
        return True

    @staticmethod
    def _target_from_prefix(prefix: str, source: str) -> Optional[dict]:
        """Rebuild a plan target from a stored partition prefix."""
        try:
            if prefix.startswith("hc/"):
                _, year_part, court_part, bench_part = prefix.split("/", 3)
                return {
                    "source": "hc",
                    "year": int(year_part.split("=", 1)[1]),
                    "court": court_part.split("=", 1)[1],
                    "bench": bench_part.split("=", 1)[1],
                }
            if prefix.startswith("sc/"):
                _, year_part, kind = prefix.split("/", 2)
                return {
                    "source": "sc",
                    "year": int(year_part.split("=", 1)[1]),
                    "court": kind,
                    "bench": None,
                }
        except (ValueError, IndexError):
            return None
        return None

    # ------------------------------------------------------------------ status

    def status(self) -> dict:
        with self._lock:
            current = dict(self.current)
        used = state.reserved_bytes()
        fs_used = state.filesystem_bytes()
        return {
            "phase": self.phase,
            "paused": self.paused,
            "auto_push": self.auto_push,
            "running": bool(self._thread and self._thread.is_alive()),
            "current": current,
            "disk": {
                "used_bytes": used,
                # Reported separately: a gap between the two means untracked
                # files are occupying staging.
                "filesystem_bytes": fs_used,
                "cap_bytes": DISK_CAP_BYTES,
                "free_bytes": max(0, DISK_CAP_BYTES - used),
                "pct": round(used / DISK_CAP_BYTES * 100, 2) if DISK_CAP_BYTES else 0,
            },
            "plan": self.plan_progress(),
            "partitions": state.partition_backlog(),
        }


worker = Worker()


def plan_from_request(
    source: str,
    year_from: int,
    year_to: int,
    courts: Optional[list[str]] = None,
    include_regional: bool = False,
) -> list[dict]:
    """Translate a dashboard scan request into a partition list."""
    return expand_targets(source, year_from, year_to, courts, include_regional)


def cleanup_orphans() -> int:
    """Delete staged files no live row claims. Returns the count removed."""
    removed = 0
    for path in state.stale_local_files():
        try:
            path.unlink()
            removed += 1
        except OSError:
            pass
    if removed:
        state.log("info", f"Cleaned up {removed} orphaned staged file(s)")
    return removed
