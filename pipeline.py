"""
Client for the tvsbackend case-law corpus API.

Target endpoint (verified against src/routes/v1/corpus.route.ts):

    POST {BACKEND_URL}/api/v1/corpus/documents
      auth:  Bearer <accessToken>, role in
             corpus_uploader | corpus_curator | admin | owner
      body:  multipart/form-data
             file      – the PDF (required, <= 50 MB, mimetype application/pdf)
             citation  – required, unique across the corpus
             title     – required
             parties, court, state, year, case_type, judges, outcome,
             language, source_url – optional
      201:   { success, data: { id, ... } }  and ingestion is queued
      409:   citation or file hash already in the corpus

The backend answers 201 as soon as the document row exists and the BullMQ
ingestion job is queued; extraction, chunking and embedding happen afterwards.
A 201 is therefore "accepted", not "indexed" — which is the right point to free
local disk, since the PDF now lives in Wasabi.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Optional

import requests

from config import (
    BACKEND_EMAIL,
    BACKEND_PASSWORD,
    BACKEND_TOKEN,
    BACKEND_URL,
    UPLOAD_TIMEOUT,
)

CORPUS_URL = f"{BACKEND_URL}/api/v1/corpus/documents"
LOGIN_URL = f"{BACKEND_URL}/api/v1/auth/login"


class PushError(Exception):
    """Upload failed in a way that is worth retrying."""


class PushRejected(Exception):
    """
    Backend refused the document permanently — a duplicate, or metadata it will
    never accept. Retrying cannot help, so the item is marked `skipped`.
    """


class PushAuthError(Exception):
    """Credentials are missing or wrong. Every upload will fail until fixed."""


class CorpusClient:
    """
    Thread-safe uploader with lazy login and one automatic re-auth per request.

    A static BACKEND_TOKEN is used as-is. With email/password the client logs in
    on first use and re-logs in once on a 401, which covers ordinary access-token
    expiry during a long batch.
    """

    def __init__(self) -> None:
        self._token: Optional[str] = BACKEND_TOKEN or None
        self._lock = threading.Lock()
        self._session = requests.Session()
        # The session keeps the accessToken cookie the first login sets, so a
        # re-auth arrives cookie-authenticated with no Authorization header —
        # exactly the case the backend's CSRF guard rejects with a 403. This
        # header satisfies it; the backend accepts X-CSRF-Token equally.
        self._session.headers["X-Requested-With"] = "XMLHttpRequest"

    # ------------------------------------------------------------------ auth

    def _login(self) -> str:
        if not (BACKEND_EMAIL and BACKEND_PASSWORD):
            raise PushAuthError(
                "No BACKEND_TOKEN and no BACKEND_EMAIL/BACKEND_PASSWORD in tracker/.env"
            )
        try:
            resp = self._session.post(
                LOGIN_URL,
                json={"email": BACKEND_EMAIL, "password": BACKEND_PASSWORD},
                timeout=30,
            )
        except requests.RequestException as exc:
            raise PushError(f"Login request failed: {exc}") from exc

        if resp.status_code != 200:
            raise PushAuthError(
                f"Login rejected ({resp.status_code}): {resp.text[:200]}"
            )

        data = resp.json().get("data") or {}
        token = data.get("accessToken")
        if not token:
            raise PushAuthError("Login succeeded but no accessToken in the response")
        return token

    def token(self, force_refresh: bool = False) -> str:
        with self._lock:
            if force_refresh and not BACKEND_TOKEN:
                self._token = None
            if self._token is None:
                self._token = self._login()
            return self._token

    def check(self) -> dict:
        """
        Verify the backend is up and the credentials work, without uploading.

        Uses the facets endpoint — cheap, and it requires the same authenticated
        session the upload does.
        """
        try:
            resp = self._session.get(
                f"{BACKEND_URL}/api/v1/corpus/facets",
                headers={"Authorization": f"Bearer {self.token()}"},
                timeout=20,
            )
        except PushAuthError as exc:
            return {"ok": False, "reason": str(exc)}
        except PushError as exc:
            # _login() wraps a refused/stalled connection in PushError. This is
            # a status probe, so report it rather than letting it 500.
            return {"ok": False, "reason": str(exc)}
        except requests.RequestException as exc:
            return {"ok": False, "reason": f"Backend unreachable: {exc}"}

        if resp.status_code == 401:
            return {"ok": False, "reason": "Token rejected (401)"}
        if resp.status_code == 403:
            return {
                "ok": False,
                "reason": "Authenticated but this user lacks a corpus role "
                          "(need corpus_uploader/corpus_curator/admin/owner)",
            }
        if resp.status_code >= 400:
            return {"ok": False, "reason": f"HTTP {resp.status_code}: {resp.text[:160]}"}
        return {"ok": True, "reason": "Backend reachable and authorised"}

    # ---------------------------------------------------------------- upload

    def push(self, item: dict, path: Path) -> int:
        """
        Upload one judgment PDF plus its metadata.

        Returns the created corpus document id. Raises PushRejected for
        permanent refusals, PushError for anything transient.
        """
        if not path.exists():
            raise PushRejected(f"Local file vanished before upload: {path}")

        fields = {
            "citation": item.get("citation") or "",
            "title": item.get("title") or "",
            "parties": item.get("parties"),
            "court": item.get("court"),
            "state": item.get("state_name"),
            "year": item.get("year"),
            "case_type": item.get("case_type"),
            "judges": item.get("judges"),
            "outcome": item.get("outcome"),
            "language": item.get("language") or "en",
            "source_url": item.get("source_url"),
        }
        # The backend's zod schema rejects an empty string where it expects a
        # URL or a number, so omit blanks instead of sending them.
        data = {k: str(v) for k, v in fields.items() if v not in (None, "")}

        # source_url must parse as a URL; the HC form carries a CNR annotation
        # that would fail validation, so drop it if it is not a bare URL.
        url = data.get("source_url", "")
        if url and (" " in url or not url.startswith("http")):
            data.pop("source_url")

        resp: Optional[requests.Response] = None
        for attempt in (1, 2):
            try:
                with path.open("rb") as handle:
                    resp = self._session.post(
                        CORPUS_URL,
                        headers={"Authorization": f"Bearer {self.token()}"},
                        data=data,
                        files={"file": (path.name, handle, "application/pdf")},
                        timeout=UPLOAD_TIMEOUT,
                    )
            except requests.RequestException as exc:
                raise PushError(f"Upload request failed: {exc}") from exc

            if resp.status_code == 401 and attempt == 1 and not BACKEND_TOKEN:
                self.token(force_refresh=True)
                continue
            break

        if resp is None:
            raise PushError("Upload produced no response")

        if resp.status_code in (200, 201):
            body = resp.json()
            doc = body.get("data") or {}
            doc_id = doc.get("id")
            if doc_id is None:
                raise PushError(f"Upload accepted but no document id: {body}")
            return int(doc_id)

        message = resp.text[:300]
        try:
            message = resp.json().get("message", message)
        except ValueError:
            pass

        if resp.status_code == 409:
            raise PushRejected(f"Already in corpus: {message}")
        if resp.status_code == 401:
            raise PushAuthError(f"Unauthorised: {message}")
        if resp.status_code == 403:
            raise PushAuthError(f"Forbidden — user lacks a corpus role: {message}")
        if resp.status_code == 400:
            # Bad metadata for this specific document; retrying sends the same
            # payload, so treat it as permanent and surface the reason.
            raise PushRejected(f"Rejected (400): {message}")
        raise PushError(f"HTTP {resp.status_code}: {message}")


client = CorpusClient()
