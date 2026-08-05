"""
FastAPI service behind the tracker dashboard.

Run it with:

    tracker-venv/bin/python tracker/api.py

then open http://127.0.0.1:8787.

The worker thread starts with the process but stays paused until you install a
scan plan and hit Resume, so launching the server never begins downloading on
its own.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

import state
from config import API_HOST, API_PORT, BACKEND_URL, DISK_CAP_BYTES, STAGING_DIR
from pipeline import client
from sources import courts as list_courts
from sources import years as list_years
from worker import cleanup_orphans, plan_from_request, worker

UI_DIR = Path(__file__).resolve().parent / "ui"

#: The closed set of states the API will filter on.
VALID_STATUSES = {
    "discovered", "downloading", "downloaded", "uploading",
    "pushed", "failed", "skipped",
}

@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    state.init()
    worker.start()
    # Always start held, whatever the flag said last run, so restarting the
    # server never resumes downloading behind your back.
    if not worker.paused:
        worker.pause("startup — press Resume to begin")
    try:
        yield
    finally:
        worker.stop()


app = FastAPI(title="Judgment Download Tracker", version="1.0.0", lifespan=lifespan)


# ------------------------------------------------------------------- schemas


class ScanRequest(BaseModel):
    source: str = Field(pattern="^(hc|sc)$")
    year_from: int = Field(ge=1950, le=2100)
    year_to: int = Field(ge=1950, le=2100)
    courts: Optional[list[str]] = None
    include_regional: bool = False
    replace: bool = True


class AutoPushRequest(BaseModel):
    enabled: bool


# ---------------------------------------------------------------- status APIs


def snapshot() -> dict:
    return {
        "worker": worker.status(),
        "stats": state.stats(),
        "backend_url": BACKEND_URL,
        "staging_dir": str(STAGING_DIR),
        "cap_bytes": DISK_CAP_BYTES,
    }


@app.get("/api/status")
def get_status() -> dict:
    return snapshot()


@app.get("/api/events")
def get_events(limit: int = 80) -> dict:
    return {"events": state.recent_events(min(limit, 500))}


@app.get("/api/items")
def get_items(status: Optional[str] = None, limit: int = 50, offset: int = 0) -> dict:
    if status and status not in VALID_STATUSES:
        raise HTTPException(400, f"Unknown status '{status}'")
    return {
        "items": state.list_items(status, min(limit, 200), max(offset, 0)),
        "status": status,
        "offset": offset,
    }


@app.get("/api/partitions")
def get_partitions() -> dict:
    return {"partitions": state.scanned_partitions()}


# --------------------------------------------------------------- catalogue


@app.get("/api/catalogue/years")
def catalogue_years(source: str = "hc") -> dict:
    if source not in ("hc", "sc"):
        raise HTTPException(400, "source must be hc or sc")
    try:
        return {"years": list_years(source)}
    except Exception as exc:
        raise HTTPException(502, f"Could not list S3 years: {exc}") from exc


@app.get("/api/catalogue/courts")
def catalogue_courts(year: int) -> dict:
    try:
        return {"courts": list_courts(year)}
    except Exception as exc:
        raise HTTPException(502, f"Could not list S3 courts: {exc}") from exc


# ----------------------------------------------------------------- controls


@app.post("/api/scan")
def post_scan(req: ScanRequest) -> dict:
    if req.year_to < req.year_from:
        raise HTTPException(400, "year_to must be >= year_from")
    try:
        targets = plan_from_request(
            req.source, req.year_from, req.year_to, req.courts, req.include_regional
        )
    except Exception as exc:
        raise HTTPException(502, f"Failed to enumerate S3 partitions: {exc}") from exc

    queued = worker.set_plan(targets, replace=req.replace)
    state.log(
        "info",
        f"Scan plan installed: {queued} new partition(s) "
        f"({req.source.upper()} {req.year_from}-{req.year_to})",
    )
    return {"planned": queued, "found": len(targets), "plan": worker.plan_progress()}


@app.post("/api/pause")
def post_pause() -> dict:
    worker.pause("dashboard")
    return worker.status()


@app.post("/api/resume")
def post_resume() -> dict:
    worker.resume()
    return worker.status()


@app.post("/api/auto-push")
def post_auto_push(req: AutoPushRequest) -> dict:
    worker.set_auto_push(req.enabled)
    return worker.status()


@app.post("/api/push-now")
def post_push_now() -> dict:
    """
    Force a push pass even with auto-push off — the "clear the batch" button.

    Enables auto-push for the duration by unpausing; the worker prioritises
    pushes over downloads, so staged files drain first and the cap frees up.
    """
    if not worker.auto_push:
        worker.set_auto_push(True)
    worker.resume()
    state.log("info", "Manual push requested")
    return worker.status()


@app.post("/api/retry-failed")
def post_retry() -> dict:
    n = state.retry_failed()
    np = state.retry_failed_partitions()
    state.log("info", f"Requeued {n} failed item(s), {np} failed partition(s)")
    worker.nudge()
    return {"requeued_items": n, "requeued_partitions": np}


@app.post("/api/cleanup")
def post_cleanup() -> dict:
    return {"removed": cleanup_orphans()}


@app.get("/api/check-backend")
def get_check_backend() -> dict:
    return client.check()


# --------------------------------------------------------------- live socket


@app.websocket("/ws")
async def ws_status(socket: WebSocket) -> None:
    """Push a status+events snapshot every second while the page is open."""
    await socket.accept()
    try:
        while True:
            payload = json.dumps({
                **snapshot(),
                "events": state.recent_events(40),
            })
            await socket.send_text(payload)
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        return
    except Exception:
        # A dead socket is not an error worth logging on every page close.
        return


# ----------------------------------------------------------------------- UI


@app.get("/")
def index() -> FileResponse:
    return FileResponse(UI_DIR / "index.html")


if UI_DIR.exists():
    app.mount("/ui", StaticFiles(directory=UI_DIR), name="ui")


if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT, log_level="info")
