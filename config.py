"""
Configuration for the judgment download tracker.

Everything tunable lives here. Values come from tracker/.env when present,
falling back to the defaults below. See .env.example for the keys that matter.
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def _load_dotenv() -> None:
    """Minimal .env reader — avoids a dependency for five keys."""
    env_file = ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_dotenv()


def _int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, "") or default)
    except ValueError:
        return default


# ---------------------------------------------------------------- storage caps

#: Hard ceiling on bytes held on local disk at once. The worker refuses to start
#: a download that would cross this line, so the staging dir never exceeds it.
DISK_CAP_BYTES = _int("DISK_CAP_BYTES", 1 * 1024 * 1024 * 1024)  # 1 GiB

#: Staging area. One subdirectory per batch; files are deleted after a
#: successful push so quota frees up incrementally.
STAGING_DIR = Path(os.environ.get("STAGING_DIR") or (ROOT / "staging"))

#: SQLite file holding every unit of work and its state. This *is* the resume
#: mechanism — kill the process at any point and restart from it.
DB_PATH = Path(os.environ.get("DB_PATH") or (ROOT / "state.db"))

# ------------------------------------------------------------------ S3 sources

AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")

HC_BUCKET = os.environ.get("HC_BUCKET", "indian-high-court-judgments")
SC_BUCKET = os.environ.get("SC_BUCKET", "indian-supreme-court-judgments")

#: Public Open Data buckets are read with unsigned requests. Set to "0" only if
#: you have credentials that must be used instead.
S3_UNSIGNED = os.environ.get("S3_UNSIGNED", "1") != "0"

#: Skip any PDF larger than this — the corpus API's multer limit is 50 MB, so a
#: bigger file is guaranteed to be rejected downstream.
MAX_PDF_BYTES = _int("MAX_PDF_BYTES", 50 * 1024 * 1024)

#: Ignore suspiciously tiny objects; a few hundred bytes is never a judgment.
MIN_PDF_BYTES = _int("MIN_PDF_BYTES", 2048)

# ------------------------------------------------------------ ingestion target

#: tvsbackend base URL. The corpus routes hang off {BACKEND_URL}/api/v1/corpus.
BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:5000").rstrip("/")

#: Either supply a long-lived access token directly...
BACKEND_TOKEN = os.environ.get("BACKEND_TOKEN", "").strip()

#: ...or credentials, and the tracker logs in and refreshes on 401.
BACKEND_EMAIL = os.environ.get("BACKEND_EMAIL", "").strip()
BACKEND_PASSWORD = os.environ.get("BACKEND_PASSWORD", "").strip()

#: Seconds to wait on a single upload. Ingestion is queued asynchronously by the
#: backend, so this only covers the multipart POST itself.
UPLOAD_TIMEOUT = _int("UPLOAD_TIMEOUT", 180)

# ------------------------------------------------------------------- behaviour

#: Parallel S3 downloads. Reading from S3 costs the eCourts portal nothing, but
#: keep it sane so the local disk and NIC are not saturated.
DOWNLOAD_WORKERS = _int("DOWNLOAD_WORKERS", 4)

#: Parallel uploads into the corpus API. Each one triggers PDF extraction and
#: embedding downstream, so stay low.
UPLOAD_WORKERS = _int("UPLOAD_WORKERS", 2)

#: Times a failed download/upload is retried before the item is parked as
#: `failed`. Failed items are retryable from the dashboard.
MAX_ATTEMPTS = _int("MAX_ATTEMPTS", 3)

#: Times a partition scan is retried (with backoff) before it is parked as
#: `failed`. A parked partition blocks "run complete" until retried or cleared.
MAX_SCAN_ATTEMPTS = _int("MAX_SCAN_ATTEMPTS", 4)

#: Finite S3 socket timeouts. Without these a network stall blocks a listing or
#: download forever and pause cannot interrupt it.
S3_CONNECT_TIMEOUT = _int("S3_CONNECT_TIMEOUT", 15)
S3_READ_TIMEOUT = _int("S3_READ_TIMEOUT", 60)

#: When true, a filled batch pushes to the pipeline automatically and the
#: downloader resumes as space frees. When false the worker stops at the cap and
#: waits for a manual push from the dashboard.
AUTO_PUSH = os.environ.get("AUTO_PUSH", "1") != "0"

#: HTTP port for the dashboard.
API_PORT = _int("API_PORT", 8787)
API_HOST = os.environ.get("API_HOST", "127.0.0.1")

STAGING_DIR.mkdir(parents=True, exist_ok=True)
