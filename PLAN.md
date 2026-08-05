# Judgment Tracker: Reliability Plan

## Purpose

This project discovers Indian High Court (HC) and Supreme Court (SC) judgment
PDFs from public AWS S3 buckets, stages them locally, and uploads each PDF plus
metadata to the TVS corpus API.

The intended workflow is:

```text
choose years/courts
  -> enumerate S3 partitions
  -> list PDF objects + load metadata index
  -> create durable work items in SQLite
  -> download PDFs into staging
  -> upload PDFs to the corpus API
  -> confirm success and remove local copy
```

The project should not be used for a large ingestion run until the critical
reliability issues below are fixed and the acceptance tests pass.

## Validated S3 source inventory

### Buckets

| Source | Bucket | Coverage |
|---|---|---|
| High Court judgments | `indian-high-court-judgments` | 1950–2026 |
| Supreme Court judgments | `indian-supreme-court-judgments` | 1950–2026 |

Both buckets are publicly listable with unsigned S3 requests and have `data/`
and `metadata/` roots.

### Content types

| Asset | HC location | SC location | Purpose |
|---|---|---|---|
| Individual PDFs | `data/pdf/year=YYYY/court=.../bench=.../` | `data/pdf/year=YYYY/{english,regional}/` | Canonical judgment content |
| Bulk PDF archives | `data/tar/.../*.tar` | `data/tar/.../*.tar` | Bulk-download alternative |
| PDF archive manifests | `data/tar/.../*.index.json` | `data/tar/.../*.index.json` | Archive contents and sizes |
| Per-document JSON | `metadata/json/.../*.json` | `metadata/json/year=YYYY/*.json` | Scraped source metadata |
| Metadata Parquet | `metadata/parquet/.../metadata.parquet` | `metadata/parquet/year=YYYY/metadata.parquet` | Efficient metadata index |
| Metadata archives | `metadata/tar/...` | `metadata/tar/...` | Bulk metadata |
| Case-detail Parquet | `metadata/parquet_case_details/...` | — | Extra HC case metadata |

There are **no standalone extracted judgment-text, OCR, or `.txt` files** in
the verified layouts. The actual judgment text remains inside the PDFs. The
`raw_html` fields are source-listing HTML, not extracted judgment text.

### Verified metadata fields

HC `metadata.parquet` includes:

```text
court_code, title, description, judge, pdf_link, cnr,
date_of_registration, decision_date, disposal_nature, court,
raw_html, pdf_exists
```

SC `metadata.parquet` includes:

```text
title, petitioner, respondent, description, judge, author_judge,
citation, case_id, cnr, decision_date, disposal_nature, court,
available_languages, raw_html, path, nc_display, scraped_at, year
```

HC case-detail Parquet additionally includes case number/type, advocates,
acts, hearings, linked cases, document references, filing/decision dates, and
disposal data.

### Source observations

- HC paths are partitioned by year, court, and bench. Example PDF names look
  like `BRHC010000012026_1_2026-01-27.pdf`.
- SC paths are partitioned by year and language. Example English names look
  like `2026_1_119_129_EN.pdf`.
- A 2026 HC sample Parquet file had 58,193 metadata rows and was about 21 MB.
- HC's `pdf_exists` metadata flag is not reliable: it can be `false` when a
  corresponding S3 PDF object exists. The actual object listing must remain
  the source of truth.

## Current state

- The local `state.db` currently has zero work items and zero scanned
  partitions, so no end-to-end ingestion has yet been demonstrated.
- The project passes Python compilation, but compilation does not exercise S3,
  staging, retry, or corpus upload behavior.
- The server starts paused by design; it will not download until a scan plan is
  installed and the worker is resumed.

## Confirmed reliability issues

### P0 — fix before any bulk run

#### 1. Failed-upload files are not accounted for and can be deleted as “orphans”

**Where:** `state.py` (`ON_DISK_STATES`, `bytes_on_disk`, `stale_local_files`)

An upload that exhausts retries becomes `failed`, but keeps `local_path` so it
can be retried. `failed` is excluded from disk accounting and from the set of
claimed staging files.

Consequences:

- The displayed 1 GiB cap can under-report real staging use.
- New downloads can exceed the intended staging capacity.
- The dashboard's **Clean orphans** action can delete a failed upload's PDF.
- Retrying that row later finds a non-existent file and can mark it skipped.

**Fix direction:** define local ownership based on an existing `local_path`,
not solely on the item status. Include retryable failed uploads in disk usage
and orphan protection. Before retrying an upload, verify the file exists; if it
does not, reset the item cleanly to the download stage rather than treating it
as a permanent rejection.

#### 2. Staging file names can collide

**Where:** `worker.py`, `_download_one`

The destination is based on `batch/source/<basename>`. Court and bench are not
part of the filename. Different S3 objects with the same basename in the same
source/batch can overwrite one another.

Consequences:

- A row can be marked downloaded while its file has been overwritten.
- The wrong PDF can be uploaded under another judgment's metadata.
- More than one row may be marked pushed even though only one PDF survived.

**Fix direction:** derive a safe, deterministic file path from the complete S3
key or item ID, preserving enough hierarchy to prevent collisions. Write to a
unique temporary path, then atomically rename it after a successful download.

#### 3. Partition scan errors are silently removed from the active plan

**Where:** `worker.py`, `_scan_round`

On any S3 scan exception, the worker logs the error and increments the plan
position. It does not retry the partition or expose it as incomplete.

Consequences:

- A transient S3 outage can cause a plan to finish with missing years/courts.
- The worker reports `idle`, which looks like completion.

**Fix direction:** store partition status and attempts. Retry transient scan
errors with bounded backoff. Mark a partition failed only after its retry
limit, show it in the dashboard, and make “run complete” require no pending or
failed partitions.

#### 4. The disk cap is not an actual staging-directory cap

**Where:** `state.py`, `worker.py`

The cap sums database `bytes` only for `downloaded` and `uploading` rows. It
does not count `.part` files, retained failed uploads, files left after a crash,
or any other physical staging files.

**Fix direction:** calculate capacity from actual staging files, or reconcile
filesystem and database ownership at startup and before every claim. Reserve
the expected bytes for `downloading` rows as well. Report database bytes and
filesystem bytes separately if they differ.

### P1 — fix for safe recovery and correctness

#### 5. Crash windows can leave inaccurate state

**Where:** `state.py`, `worker.py`

If the process stops after a file is written but before SQLite is updated, the
PDF becomes an untracked file. If it stops after the corpus accepts an upload
but before `mark_pushed`, a retry may encounter a duplicate or a missing local
file and record the wrong terminal status.

**Fix direction:** use explicit recovery states and reconciliation:

1. Persist a unique staging path before starting the download.
2. On startup, check each in-flight row against the filesystem.
3. Keep an upload receipt/response before deleting the local file.
4. Treat backend duplicate responses as successful only when they can be
   correlated to the same source object/citation.
5. Run automatic orphan reconciliation on startup; do not blindly delete
   ambiguous files.

#### 6. S3 calls have no explicit network timeouts

**Where:** `sources.py`, `s3()`

The S3 client specifies retry behavior but no connection/read timeouts. A
network stall may block listing or downloading for an unbounded period and a
pause cannot interrupt the in-flight request.

**Fix direction:** set finite `connect_timeout` and `read_timeout` in the
botocore config. Surface timeout failures as retryable errors and add a
heartbeat/progress timestamp to the dashboard.

#### 7. Multiple worker processes are unsafe

**Where:** `state.py`, `worker.py`, `cli.py`, `api.py`

The API process and CLI can each start a worker. The Python write lock is
process-local, and `downloading` reservations are excluded from the disk
budget. Multiple processes can reserve the same apparent free capacity.

**Fix direction:** enforce one worker with an OS-level process lock, or move
claiming and byte reservations entirely into SQLite so they are safe across
processes. Document that `api.py` and `cli.py run` must not be run together
until this is fixed.

#### 8. Citation uniqueness may discard valid SC documents

**Where:** `state.py`, `sources.py`

The local `items.citation` index is unique and `add_items` uses `INSERT OR
IGNORE`. A repeated reported citation can silently discard a distinct source
PDF, including version/language variants.

**Fix direction:** use `s3_key` as the tracker’s only local identity. Generate
a stable, guaranteed-unique ingestion citation from source + complete object
identity, while retain the reported citation in a separate metadata field.

### P2 — improve operational clarity

#### 9. Batch lifecycle is incomplete

Batch statuses support `open`, `full`, `pushing`, and `done`, but normal push
completion does not close a batch or mark it done.

**Fix direction:** update batch state after every push pass and close it when
all owned items are terminal. This is primarily reporting/operability work.

#### 10. Metadata reads hide errors

**Where:** `sources.py`, `_read_parquet`

Parquet read failures return `None`, then discovery continues with fallback
metadata. This avoids halting, but can ingest low-quality records without a
clear warning.

**Fix direction:** distinguish “metadata absent” from “metadata fetch/read
failed.” Retry the latter and display it; decide explicitly whether PDF-only
ingestion is allowed for a partition.

## Implementation order

1. Add tests and a small local fake S3/fake corpus environment.
2. Fix local file ownership, disk accounting, and cleanup safety (P0.1/P0.4).
3. Make staging paths collision-proof (P0.2).
4. Add durable scan retry/status handling (P0.3).
5. Add recovery/reconciliation for interrupted downloads/uploads (P1.5).
6. Add S3 timeouts and worker heartbeat (P1.6).
7. Prevent concurrent workers or make reservations cross-process safe (P1.7).
8. Separate source identity from corpus citation (P1.8).
9. Improve batch completion and metadata-error reporting.
10. Perform a small real S3 pilot before any broad scan.

## Required test coverage

### Unit tests

- Claiming work never exceeds a capacity reservation.
- Failed upload with a local PDF remains counted and is never an orphan.
- Retrying a failed upload with a missing local file re-downloads it safely.
- Two different S3 keys with the same basename use different staging paths.
- A scan failure remains pending/retryable and cannot be reported as complete.
- Citation collisions do not lose source work items.
- Startup recovery reconciles `downloading` and `uploading` rows correctly.

### Integration tests with fakes

- S3 download succeeds, upload succeeds, and the PDF is removed only after the
  successful corpus response has been durably recorded.
- Upload timeout retries without re-downloading the file.
- Authentication failure pauses safely without corrupting or discarding work.
- Backend accepts an upload but the process stops before state finalization;
  restart resolves it deterministically.
- Disk-full and cleanup scenarios preserve all retryable files.

### Real S3 pilot

Use one small HC bench or a single SC year and a deliberately small capacity.
Verify:

1. All enumerated PDFs become either `pushed`, intentionally `skipped`, or
   explicitly `failed` with a visible reason.
2. Filesystem staging usage never exceeds the configured cap.
3. Restarting during download and during upload does not lose or duplicate a
   document.
4. A backend duplicate is reported accurately and does not falsely hide a
   distinct source object.
5. Final local staging directory is empty except for deliberately retained
   failed/retryable files, all of which appear in the dashboard.

## Definition of ready for a bulk run

The tracker is ready only when:

- all P0 fixes are implemented and tested;
- the P1 recovery path is tested at least once;
- one real S3 pilot completes without unexplained missing documents;
- measured filesystem usage respects the configured limit;
- no failed item can be destroyed by cleanup;
- the operator can see incomplete/failed partitions and retry them;
- only one worker process can claim work at a time; and
- corpus authentication has been checked before downloading a large batch.

## Non-goals for the first repair pass

- PDF OCR or text extraction. The tracker should upload source PDFs; corpus
  ingestion can handle extraction downstream.
- Replacing individual object downloads with TAR archive extraction. Tar files
  are useful for future throughput optimization, but individual PDFs are the
  simpler and safer path while correctness issues are being repaired.
- Expanding metadata taxonomy beyond the available S3 fields.
