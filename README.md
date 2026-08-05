# Judgment Download Tracker

Discovers Indian High Court and Supreme Court judgment PDFs from the public AWS
Open Data buckets, stages them locally under a hard disk cap, and uploads each
PDF plus its metadata to the TVS corpus API.

```text
choose years/courts
  -> enumerate S3 partitions
  -> list PDF objects + load metadata index
  -> create durable work items in SQLite
  -> download PDFs into staging
  -> upload PDFs to the corpus API
  -> confirm success and remove local copy
```

SQLite (`state.db`) is the single source of truth. Killing the process at any
point is safe: finished downloads and pushes are already committed, and anything
mid-flight is reconciled against the filesystem on the next start.

## Sources

| Source | Bucket | Coverage |
|---|---|---|
| High Court judgments | `indian-high-court-judgments` | 1950–2026 |
| Supreme Court judgments | `indian-supreme-court-judgments` | 1950–2026 |

Both are publicly listable with unsigned S3 requests. Nothing here writes to S3
or touches the eCourts portals.

## Requirements

- Python 3.10+
- A reachable TVS backend with a user holding a corpus role
  (`corpus_uploader`, `corpus_curator`, `admin`, or `owner`)

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# then edit .env — at minimum BACKEND_URL plus either BACKEND_TOKEN
# or BACKEND_EMAIL/BACKEND_PASSWORD
```

`.env` is gitignored; it holds credentials and must never be committed.

## Running the dashboard

```bash
python3 api.py
```

Open the host/port from your `.env` (`API_HOST`/`API_PORT`, default
`http://127.0.0.1:8787`).

The worker starts **paused** on purpose, so launching the server never begins
downloading on its own. Order of operations:

1. **Test backend** — confirm the API is reachable and authorised.
2. Pick a source, year range, and court, then **Queue partitions**.
3. **Resume** to start scanning, downloading, and pushing.

Verify the backend before queueing a large range; an auth failure part-way
through pauses the worker, but checking first is cheaper.

## Running headless

```bash
python3 cli.py status     # counts, disk usage, plan progress
python3 cli.py check      # verify backend reachable + authorised
python3 cli.py scan --source hc --from 2024 --to 2024 --court 11_24
python3 cli.py run        # work until the plan is exhausted
python3 cli.py run --once # one pass, then exit
python3 cli.py retry      # requeue failed items and failed partitions
python3 cli.py items --status failed
python3 cli.py cleanup    # delete staged files no item claims
```

`run` obeys the same disk cap and pause flag as the dashboard. Ctrl-C is safe at
any point; a second Ctrl-C forces exit.

Only one worker may run at a time — an OS-level lock at `staging/.worker.lock`
enforces this, so `api.py` and `cli.py run` cannot both claim work.

## How the disk cap works

`DISK_CAP_BYTES` (default 1 GiB) is a ceiling on staged bytes. Capacity is
charged against every item that owns or is promised a local file — `downloaded`,
`uploading`, `failed` (retained for retry), and in-flight `downloading`. The
budget is recomputed inside the same write transaction that claims work, so
concurrent download threads cannot both spend the same free space.

The dashboard reports database-derived usage and actual filesystem usage
separately. A gap between them means untracked files are occupying staging;
**Clean orphans** lists and removes files no row claims. Files belonging to
retryable failed uploads are never treated as orphans.

## Failure handling

- **Failed downloads/uploads** retry up to `MAX_ATTEMPTS`, then park as `failed`
  and stay retryable from the dashboard or `cli.py retry`. A failed upload keeps
  its PDF; if that file goes missing, the item resets to the download stage
  rather than being rejected.
- **Failed partition scans** retry with exponential backoff up to
  `MAX_SCAN_ATTEMPTS`, then park as `failed`. A plan with pending or failed
  partitions reports `scan-incomplete`, not `idle`, so a transient S3 outage
  cannot look like a finished run.
- **Metadata read failures** are distinguished from absent metadata and logged;
  PDFs are still ingested, without the metadata fields.

## Status

Reliability work from `PLAN.md` is implemented, but **not yet covered by tests**
and no real S3 pilot has been run end to end. Before a bulk ingestion run, do a
small pilot — one HC bench or a single SC year, with a deliberately small
`DISK_CAP_BYTES` — and confirm every enumerated PDF ends as `pushed`,
intentionally `skipped`, or explicitly `failed` with a visible reason. See
`PLAN.md` for the full acceptance checklist.

## Layout

| File | Role |
|---|---|
| `api.py` | FastAPI dashboard and control endpoints |
| `cli.py` | Headless control over the same engine |
| `worker.py` | Scan/download/push loop |
| `state.py` | SQLite schema, migrations, and all state transitions |
| `sources.py` | S3 discovery and metadata normalisation |
| `pipeline.py` | Corpus API client |
| `config.py` | Configuration, loaded from `.env` |
