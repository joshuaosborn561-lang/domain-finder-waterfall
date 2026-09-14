"""Background jobs. get_job_status always returns progress, never a bare error."""

from __future__ import annotations

import json
import threading
import time
import traceback
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
JOBS_DIR = ROOT / "data" / "jobs"

STALL_SECONDS = 300.0
STALL_POLL_SECONDS = 15.0
STALL_REASON = "stalled: no progress for 5 minutes"


@dataclass
class Job:
    id: str
    kind: str
    status: str
    created_at: float
    started_at: float | None = None
    finished_at: float | None = None
    error: str | None = None
    result: dict[str, Any] = field(default_factory=dict)
    meta: dict[str, Any] = field(default_factory=dict)

    def to_public(self) -> dict[str, Any]:
        return asdict(self)


_lock = threading.Lock()
_jobs: dict[str, Job] = {}
_cancels: dict[str, threading.Event] = {}


def _path(job_id: str) -> Path:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    return JOBS_DIR / f"{job_id}.json"


def _persist(job: Job) -> None:
    _path(job.id).write_text(json.dumps(job.to_public(), indent=2, default=str), encoding="utf-8")


def is_cancelled(job_id: str) -> bool:
    ev = _cancels.get(job_id)
    return bool(ev and ev.is_set())


def _parse_progress_ts(job: Job) -> float:
    raw = (job.result or {}).get("last_progress_at")
    if isinstance(raw, (int, float)) and raw > 0:
        return float(raw)
    if isinstance(raw, str) and raw.strip():
        text = raw.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(text).timestamp()
        except ValueError:
            pass
    return float(job.started_at or job.created_at)


def request_cancel(job_id: str, reason: str) -> None:
    ev = _cancels.get(job_id)
    if ev:
        ev.set()
    with _lock:
        job = _jobs.get(job_id)
        if job is None or job.status in ("completed", "failed"):
            return
        job.status = "failed"
        job.error = reason
        snap = dict(job.result or {})
        snap["status"] = "failed"
        snap["error"] = reason
        snap["progress"] = "failed"
        job.result = snap
        job.finished_at = time.time()
    _persist(job)


def get_job(job_id: str) -> dict[str, Any]:
    """Never raise. Unknown ids return last-known-style progress with status=unknown."""
    job_id = (job_id or "").strip()
    if not job_id:
        return {"ok": False, "status": "unknown", "error": "job_id is required", "result": {}}
    with _lock:
        job = _jobs.get(job_id)
    if job is None:
        path = _path(job_id)
        if not path.exists():
            return {
                "ok": True,
                "job_id": job_id,
                "status": "unknown",
                "result": {"progress": "no job recorded on this process"},
                "error": None,
            }
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            job = Job(**data)
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": True,
                "job_id": job_id,
                "status": "unknown",
                "result": {"progress": "job file unreadable"},
                "error": str(exc),
            }
        with _lock:
            _jobs[job.id] = job
    public = job.to_public()
    public["ok"] = True
    public["job_id"] = job.id
    return public


def list_jobs(limit: int = 20) -> list[dict[str, Any]]:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted(JOBS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    out: list[dict[str, Any]] = []
    for path in files[: max(1, min(int(limit or 20), 100))]:
        out.append(get_job(path.stem))
    return out


def update_job_progress(job_id: str, snapshot: dict[str, Any]) -> None:
    if is_cancelled(job_id):
        return
    with _lock:
        job = _jobs.get(job_id)
        if job is None or job.status == "failed":
            return
        job.result = dict(snapshot)
    _persist(job)


def _stall_watch(job_id: str, cancel: threading.Event) -> None:
    while not cancel.wait(STALL_POLL_SECONDS):
        with _lock:
            job = _jobs.get(job_id)
        if job is None or job.status != "running":
            return
        age = time.time() - _parse_progress_ts(job)
        if age >= STALL_SECONDS:
            request_cancel(job_id, STALL_REASON)
            return


def start_job(
    kind: str,
    fn: Callable[[Job], dict[str, Any]],
    meta: dict[str, Any] | None = None,
) -> Job:
    job = Job(
        id=uuid.uuid4().hex[:12],
        kind=kind,
        status="queued",
        created_at=time.time(),
        meta=meta or {},
        result={"progress": "queued"},
    )
    cancel = threading.Event()
    with _lock:
        _jobs[job.id] = job
        _cancels[job.id] = cancel
    _persist(job)

    def worker() -> None:
        job.status = "running"
        job.started_at = time.time()
        job.result = {
            "progress": "running",
            "last_progress_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
        }
        _persist(job)
        threading.Thread(
            target=_stall_watch, args=(job.id, cancel), name=f"dw-stall-{job.id}", daemon=True
        ).start()
        try:
            result = fn(job) or {"progress": "completed"}
            if cancel.is_set() or job.status == "failed":
                return
            job.result = result
            job.status = "completed"
        except Exception as exc:  # noqa: BLE001
            if cancel.is_set() or job.status == "failed":
                return
            job.status = "failed"
            job.error = f"{type(exc).__name__}: {exc}"
            job.result = {
                "progress": "failed",
                "error": job.error,
                "traceback": traceback.format_exc()[-2000:],
            }
        finally:
            cancel.set()
            if job.finished_at is None:
                job.finished_at = time.time()
            _persist(job)

    threading.Thread(target=worker, name=f"dw-job-{job.id}", daemon=True).start()
    return job
