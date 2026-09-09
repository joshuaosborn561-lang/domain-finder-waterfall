"""Background jobs. get_job_status always returns progress, never a bare error."""

from __future__ import annotations

import json
import threading
import time
import traceback
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parent.parent
JOBS_DIR = ROOT / "data" / "jobs"


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


def _path(job_id: str) -> Path:
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    return JOBS_DIR / f"{job_id}.json"


def _persist(job: Job) -> None:
    _path(job.id).write_text(json.dumps(job.to_public(), indent=2, default=str), encoding="utf-8")


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
    with _lock:
        job = _jobs.get(job_id)
        if job is None:
            return
        job.result = dict(snapshot)
    _persist(job)


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
    with _lock:
        _jobs[job.id] = job
    _persist(job)

    def worker() -> None:
        job.status = "running"
        job.started_at = time.time()
        job.result = {"progress": "running"}
        _persist(job)
        try:
            job.result = fn(job) or {"progress": "completed"}
            job.status = "completed"
        except Exception as exc:  # noqa: BLE001
            job.status = "failed"
            job.error = f"{type(exc).__name__}: {exc}"
            job.result = {
                "progress": "failed",
                "error": job.error,
                "traceback": traceback.format_exc()[-2000:],
            }
        finally:
            job.finished_at = time.time()
            _persist(job)

    threading.Thread(target=worker, name=f"dw-job-{job.id}", daemon=True).start()
    return job
