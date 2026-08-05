"""
Discovery against the two AWS Open Data buckets.

Both datasets are laid out as Hive-style partitions with the PDFs and the
metadata in parallel prefixes. Verified layouts:

  High Court — s3://indian-high-court-judgments
    data/pdf/year=YYYY/court=SS_CC/bench=NAME/<CNR>_<n>_<date>.pdf
    metadata/parquet/year=YYYY/court=SS_CC/bench=NAME/metadata.parquet
      columns: court_code, title, description, judge, pdf_link, cnr,
               date_of_registration, decision_date, disposal_nature, court,
               raw_html, pdf_exists

  Supreme Court — s3://indian-supreme-court-judgments
    data/pdf/year=YYYY/{english,regional}/<path>_EN.pdf
    metadata/parquet/year=YYYY/metadata.parquet
      columns: title, petitioner, respondent, description, judge, author_judge,
               citation, case_id, cnr, decision_date, disposal_nature, court,
               available_languages, raw_html, path, nc_display, scraped_at, year

Discovery reads the parquet for a partition, lists the PDF objects in the
matching data prefix, and joins them on filename. Only rows with a real PDF
object become items — the parquet's own `pdf_exists` flag is unreliable
(it is null for mobile-sourced rows and false for some rows whose PDF is
present anyway), so object listing is the authority.

Nothing here writes to S3 and nothing touches the eCourts portals.
"""

from __future__ import annotations

import io
import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Optional

import boto3
import pyarrow.parquet as pq
from botocore import UNSIGNED
from botocore.config import Config

from config import (
    AWS_REGION,
    HC_BUCKET,
    MAX_PDF_BYTES,
    MIN_PDF_BYTES,
    S3_CONNECT_TIMEOUT,
    S3_READ_TIMEOUT,
    S3_UNSIGNED,
    SC_BUCKET,
)

#: Court code -> name table, vendored from the scraper repo so the tracker
#: stands alone (upstream: vanga/indian-high-court-judgments court-codes.json).
COURT_CODES_FILE = Path(__file__).resolve().parent / "court-codes.json"

#: Case-type taxonomy the corpus API expects (mirrors corpus.config.ts).
CORPUS_CASE_TYPES = [
    "writ_petition",
    "criminal_appeal",
    "civil_appeal",
    "special_leave_petition",
    "review_petition",
    "departmental_inquiry",
    "service_matter",
    "disciplinary_proceeding",
    "corruption_case",
    "contempt_petition",
    "arbitration",
    "tax_matter",
]

#: Court code (S3 form) -> state name, for the corpus `state` column.
COURT_STATE = {
    "9_13": "Uttar Pradesh",
    "27_1": "Maharashtra",
    "19_16": "West Bengal",
    "18_6": "Assam",
    "36_29": "Telangana",
    "28_2": "Andhra Pradesh",
    "22_18": "Chhattisgarh",
    "7_26": "Delhi",
    "24_17": "Gujarat",
    "2_5": "Himachal Pradesh",
    "1_12": "Jammu and Kashmir",
    "20_7": "Jharkhand",
    "29_3": "Karnataka",
    "32_4": "Kerala",
    "23_23": "Madhya Pradesh",
    "14_25": "Manipur",
    "17_21": "Meghalaya",
    "21_11": "Odisha",
    "3_22": "Punjab and Haryana",
    "8_9": "Rajasthan",
    "11_24": "Sikkim",
    "16_20": "Tripura",
    "5_15": "Uttarakhand",
    "33_10": "Tamil Nadu",
    "10_8": "Bihar",
}


def s3():
    cfg = Config(
        signature_version=UNSIGNED if S3_UNSIGNED else "s3v4",
        retries={"max_attempts": 5, "mode": "standard"},
        max_pool_connections=32,
        connect_timeout=S3_CONNECT_TIMEOUT,
        read_timeout=S3_READ_TIMEOUT,
    )
    return boto3.client("s3", region_name=AWS_REGION, config=cfg)


@lru_cache(maxsize=1)
def court_names() -> dict[str, str]:
    """S3-form court code -> official court name, from the vendored table."""
    if not COURT_CODES_FILE.exists():
        return {}
    raw = json.loads(COURT_CODES_FILE.read_text())
    return {code.replace("~", "_"): name for code, name in raw.items()}


# ------------------------------------------------------------- prefix listing


def _list_dirs(bucket: str, prefix: str) -> list[str]:
    """Immediate sub-prefix names under `prefix` (no trailing slash)."""
    client = s3()
    names: list[str] = []
    token: Optional[str] = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for cp in resp.get("CommonPrefixes", []):
            names.append(cp["Prefix"][len(prefix):].rstrip("/"))
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return names


def _list_objects(bucket: str, prefix: str) -> dict[str, int]:
    """filename -> size for every object directly under `prefix`."""
    client = s3()
    out: dict[str, int] = {}
    token: Optional[str] = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            out[key[len(prefix):]] = int(obj["Size"])
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return out


def _read_parquet(bucket: str, key: str) -> Optional[tuple[list[dict], Optional[str]]]:
    """
    Whole-object read into row dicts. Returns (rows, error). Distinguishes
    "absent" (returns ([], None)) from "fetch/parse failed" (returns ([], error_msg)).
    """
    try:
        body = s3().get_object(Bucket=bucket, Key=key)["Body"].read()
    except s3().exceptions.NoSuchKey:
        return [], None
    except Exception as exc:
        return [], f"S3 fetch failed: {exc}"
    try:
        return pq.read_table(io.BytesIO(body)).to_pylist(), None
    except Exception as exc:
        return [], f"Parquet parse failed: {exc}"


# ----------------------------------------------------------------- catalogue


def years(source: str) -> list[int]:
    bucket = HC_BUCKET if source == "hc" else SC_BUCKET
    out = []
    for name in _list_dirs(bucket, "data/pdf/"):
        if name.startswith("year="):
            try:
                out.append(int(name.split("=", 1)[1]))
            except ValueError:
                continue
    return sorted(out)


def courts(year: int) -> list[dict]:
    """High Court codes present in a given year, with human names."""
    names = court_names()
    out = []
    for name in _list_dirs(HC_BUCKET, f"data/pdf/year={year}/"):
        if not name.startswith("court="):
            continue
        code = name.split("=", 1)[1]
        out.append({"code": code, "name": names.get(code, code)})
    return sorted(out, key=lambda c: c["name"])


def benches(year: int, court: str) -> list[str]:
    out = []
    for name in _list_dirs(HC_BUCKET, f"data/pdf/year={year}/court={court}/"):
        if name.startswith("bench="):
            out.append(name.split("=", 1)[1])
    return sorted(out)


# ------------------------------------------------------- metadata normalisation

_CASE_TYPE_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("special_leave_petition", re.compile(r"\bslp\b|special leave", re.I)),
    ("writ_petition", re.compile(r"\bw\.?p\b|\bwp\(|writ petition|art(?:icle)?\.? ?226", re.I)),
    ("criminal_appeal", re.compile(r"crl\.? ?a|criminal appeal|cr\.?a\b", re.I)),
    ("civil_appeal", re.compile(r"\bc\.?a\b|civil appeal|first appeal|\bfa\b", re.I)),
    ("review_petition", re.compile(r"review petition|\brp\(|\br\.?p\.?\b", re.I)),
    ("contempt_petition", re.compile(r"contempt", re.I)),
    ("arbitration", re.compile(r"arbitration|\barb\b|a&c|arb\.? ?p", re.I)),
    ("tax_matter", re.compile(r"income tax|\bitr\b|\bita\b|sales tax|\bgst\b|vat|excise|customs", re.I)),
    ("corruption_case", re.compile(r"prevention of corruption|\bpc act\b|corruption|\bcbi\b", re.I)),
    ("departmental_inquiry", re.compile(r"departmental (?:inquiry|enquiry)|charge ?sheet|disciplinary (?:inquiry|enquiry)", re.I)),
    ("disciplinary_proceeding", re.compile(r"disciplinary", re.I)),
    ("service_matter", re.compile(r"\boa\b|service matter|original application|\bcat\b|promotion|seniority|pension", re.I)),
]


def guess_case_type(*texts: Optional[str]) -> Optional[str]:
    """
    Map free text (case title, description) onto the corpus taxonomy.

    Returns None rather than a wrong guess when nothing matches — the corpus
    treats a null case_type as "unclassified", which is honest, whereas a
    fabricated one poisons the facet filters.
    """
    blob = " ".join(t for t in texts if t)
    if not blob:
        return None
    for case_type, pattern in _CASE_TYPE_PATTERNS:
        if pattern.search(blob):
            return case_type
    return None


def _clip(value: Optional[str], limit: int) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:limit]


def _decision_year(value) -> Optional[int]:
    """Year out of the several date shapes the two parquets use."""
    if value is None:
        return None
    text = str(value)
    m = re.search(r"(19|20)\d{2}", text)
    return int(m.group(0)) if m else None


def split_parties(title: Optional[str]) -> Optional[str]:
    """
    HC titles look like "WP(C)/60/2023 of PETITIONER Vs RESPONDENT".
    Return the party half when the pattern holds, else the whole title.
    """
    if not title:
        return None
    body = title.split(" of ", 1)[1] if " of " in title else title
    return body.strip() or None


# -------------------------------------------------------------- HC discovery


def discover_hc(year: int, court: str, bench: str) -> tuple[list[dict], int, Optional[str]]:
    """
    Build item rows for one High Court (year, court, bench) partition.

    Returns (rows, objects_seen, metadata_error). `metadata_error` distinguishes
    absent metadata from a fetch/parse failure that should be retried.
    """
    pdf_prefix = f"data/pdf/year={year}/court={court}/bench={bench}/"
    meta_key = f"metadata/parquet/year={year}/court={court}/bench={bench}/metadata.parquet"

    objects = _list_objects(HC_BUCKET, pdf_prefix)
    if not objects:
        return [], 0, None

    rows, meta_error = _read_parquet(HC_BUCKET, meta_key)
    # pdf_link is a repo-relative path; its basename is the S3 object name.
    by_name: dict[str, dict] = {}
    for row in rows:
        link = row.get("pdf_link")
        if link:
            by_name[str(link).rsplit("/", 1)[-1]] = row

    court_name = court_names().get(court, court)
    state = COURT_STATE.get(court)
    items: list[dict] = []

    for filename, size in objects.items():
        if not filename.endswith(".pdf") or "/" in filename:
            continue
        if size < MIN_PDF_BYTES or size > MAX_PDF_BYTES:
            continue
        meta = by_name.get(filename, {})
        title = _clip(meta.get("title"), 500) or Path(filename).stem
        cnr = _clip(meta.get("cnr"), 64)
        dec_year = _decision_year(meta.get("decision_date")) or year

        # Citation for corpus must be unique. S3 key is unique by construction;
        # derive a stable ingestion citation from it. Keep the reported citation
        # separately to avoid losing source metadata.
        ingestion_citation = f"HC/{court}/{bench}/{Path(filename).stem}"
        source_citation = _clip(meta.get("cnr"), 64) or ingestion_citation

        items.append({
            "source": "hc",
            "bucket": HC_BUCKET,
            "s3_key": pdf_prefix + filename,
            "bytes": size,
            "citation": ingestion_citation[:255],
            "source_citation": source_citation[:255],
            "title": title,
            "year": dec_year,
            "court": _clip(meta.get("court"), 255) or court_name,
            "state_name": state,
            "bench": bench,
            "case_type": guess_case_type(title, meta.get("description")),
            "judges": _clip(meta.get("judge"), 1000),
            "parties": _clip(split_parties(title), 2000),
            "outcome": _clip(meta.get("disposal_nature"), 5000),
            "language": "en",
            "source_url": f"https://judgments.ecourts.gov.in/ (CNR {cnr})" if cnr else None,
            "cnr": cnr,
        })

    return items, len(objects), meta_error


# -------------------------------------------------------------- SC discovery


def discover_sc(year: int, kind: str = "english") -> tuple[list[dict], int, Optional[str]]:
    """
    Build item rows for one Supreme Court (year, english|regional) partition.

    SC parquet rows carry a `path` like "2024_10_108_125"; the English PDF is
    "<path>_EN.pdf". Regional PDFs carry a language suffix instead.

    Returns (rows, objects_seen, metadata_error).
    """
    pdf_prefix = f"data/pdf/year={year}/{kind}/"
    meta_key = f"metadata/parquet/year={year}/metadata.parquet"

    objects = _list_objects(SC_BUCKET, pdf_prefix)
    if not objects:
        return [], 0, None

    rows, meta_error = _read_parquet(SC_BUCKET, meta_key)
    by_path = {str(r.get("path")): r for r in rows if r.get("path")}

    items: list[dict] = []
    for filename, size in objects.items():
        if not filename.endswith(".pdf") or "/" in filename:
            continue
        if size < MIN_PDF_BYTES or size > MAX_PDF_BYTES:
            continue

        stem = Path(filename).stem
        # "2024_10_108_125_EN" -> "2024_10_108_125"; regional uses e.g. "_HIN".
        base, _, suffix = stem.rpartition("_")
        meta = by_path.get(base) or by_path.get(stem) or {}

        title = _clip(meta.get("title"), 500) or stem
        # SC: derive ingestion citation from s3_key (guaranteed unique), keep
        # reported citation separately. Language suffix makes regionals distinct.
        ingestion_citation = f"SC/{year}/{stem}"
        source_citation = (
            _clip(meta.get("citation"), 200)
            or _clip(meta.get("nc_display"), 200)
            or _clip(meta.get("case_id"), 200)
            or ingestion_citation
        )
        if kind != "english" and suffix:
            ingestion_citation = f"{ingestion_citation}_{suffix}"

        petitioner = _clip(meta.get("petitioner"), 900)
        respondent = _clip(meta.get("respondent"), 900)
        parties = " vs ".join(p for p in (petitioner, respondent) if p) or None

        judges = " | ".join(
            j for j in (_clip(meta.get("judge"), 500), _clip(meta.get("author_judge"), 400)) if j
        ) or None

        items.append({
            "source": "sc",
            "bucket": SC_BUCKET,
            "s3_key": pdf_prefix + filename,
            "bytes": size,
            "citation": ingestion_citation[:255],
            "source_citation": source_citation[:255],
            "title": title,
            "year": _decision_year(meta.get("decision_date")) or year,
            "court": _clip(meta.get("court"), 255) or "Supreme Court of India",
            "state_name": "India",
            "bench": None,
            "case_type": guess_case_type(title, meta.get("description")),
            "judges": judges,
            "parties": _clip(parties, 2000),
            "outcome": _clip(meta.get("disposal_nature"), 5000),
            "language": "en" if kind == "english" else (suffix or "regional").lower()[:10],
            "source_url": "https://scr.sci.gov.in/",
            "cnr": _clip(meta.get("cnr"), 64),
        })

    return items, len(objects), meta_error


def partition_prefix(source: str, year: int, court: Optional[str], bench: Optional[str]) -> str:
    """Stable identity for a scanned partition (used for scan bookkeeping)."""
    if source == "hc":
        return f"hc/year={year}/court={court}/bench={bench}"
    return f"sc/year={year}/{court or 'english'}"


def expand_targets(
    source: str,
    year_from: int,
    year_to: int,
    courts_filter: Optional[Iterable[str]] = None,
    include_regional: bool = False,
) -> list[dict]:
    """
    Enumerate every partition a scan request covers.

    Listing prefixes is cheap; reading parquet and PDF listings is not, so the
    worker walks this list lazily and records progress per partition.
    """
    targets: list[dict] = []
    wanted = set(courts_filter) if courts_filter else None

    for year in range(year_from, year_to + 1):
        if source == "sc":
            kinds = ["english"] + (["regional"] if include_regional else [])
            for kind in kinds:
                targets.append({"source": "sc", "year": year, "court": kind, "bench": None})
            continue

        for court in _list_dirs(HC_BUCKET, f"data/pdf/year={year}/"):
            if not court.startswith("court="):
                continue
            code = court.split("=", 1)[1]
            if wanted and code not in wanted:
                continue
            for bench in _list_dirs(HC_BUCKET, f"data/pdf/year={year}/court={code}/"):
                if not bench.startswith("bench="):
                    continue
                targets.append({
                    "source": "hc",
                    "year": year,
                    "court": code,
                    "bench": bench.split("=", 1)[1],
                })
    return targets


def download_object(bucket: str, key: str, dest: Path) -> int:
    """
    Stream one object to disk with atomic write. Returns bytes written.

    Writes to {dest}.part then atomically renames on success. Caller must ensure
    dest path is unique to avoid collisions.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        s3().download_file(bucket, key, str(tmp))
        size = tmp.stat().st_size
        tmp.replace(dest)
        return size
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
