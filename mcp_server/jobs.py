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


def _terminal(status: str) -> bool:
    return status in ("completed", "failed", "stalled", "cancelled")


def _requests_made(job: Job) -> int:
    res = job.result or {}
    for key in ("requests_made",):
        val = res.get(key)
        if isinstance(val, (int, float)):
            return int(val)
    # Fall back to sum of finished tier request counters while a tier is in flight.
    tiers = res.get("tiers") or []
    total = 0
    if isinstance(tiers, list):
        for t in tiers:
            if isinstance(t, dict) and isinstance(t.get("requests_made"), (int, float)):
                total += int(t["requests_made"])
    cur = res.get("requests_made")
    if isinstance(cur, (int, float)):
        return int(cur)
    return total


def request_cancel(job_id: str, reason: str, *, status: str = "cancelled") -> dict[str, Any]:
    """Signal workers to stop. Flushes via should_stop; status sticks."""
    ev = _cancels.get(job_id)
    if ev:
        ev.set()
    with _lock:
        job = _jobs.get(job_id)
        if job is None:
            return {"ok": False, "job_id": job_id, "status": "unknown", "error": "job not found"}
        if _terminal(job.status):
            return job.to_public() | {"ok": True, "job_id": job.id}
        job.status = status
        job.error = reason
        snap = dict(job.result or {})
        snap["status"] = status
        snap["error"] = reason
        snap["progress"] = status
        tier = snap.get("tier") or ""
        if status == "stalled" and tier and "tier" not in reason:
            job.error = f"{reason}, tier {tier}"
            snap["error"] = job.error
        job.result = snap
        job.finished_at = time.time()
    _persist(job)
    return get_job(job_id)


def get_job(job_id: str) -> dict[str, Any]:
    """Never raise. Unknown ids return last known style progress with status=unknown."""
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
        if job is None or _terminal(job.status):
            return
        job.result = dict(snapshot)
    _persist(job)


def _stall_watch(job_id: str, cancel: threading.Event) -> None:
    last_seen_requests: int | None = None
    last_move = time.time()
    while not cancel.wait(STALL_POLL_SECONDS):
        with _lock:
            job = _jobs.get(job_id)
        if job is None or job.status != "running":
            return
        made = _requests_made(job)
        if last_seen_requests is None or made != last_seen_requests:
            last_seen_requests = made
            last_move = time.time()
            continue
        # Also require that we are inside a tier that should be making requests.
        phase = (job.result or {}).get("phase")
        tier = (job.result or {}).get("tier") or ""
        if phase == "tier" and tier in ("maps", "serp", "aiark", "discolike", "prospeo", "leadmagic"):
            if time.time() - last_move >= STALL_SECONDS:
                request_cancel(
                    job_id,
                    f"stalled, no requests_made progress for 5 minutes, tier {tier}",
                    status="stalled",
                )
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
        result={"progress": "queued", "requests_made": 0},
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
            "requests_made": 0,
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
            if cancel.is_set() or _terminal(job.status):
                # Keep stalled/cancelled status; merge partial result if richer.
                if isinstance(result, dict) and result.get("tiers"):
                    with _lock:
                        merged = dict(job.result or {})
                        merged.update(result)
                        merged["status"] = job.status
                        if job.error:
                            merged["error"] = job.error
                        job.result = merged
                return
            job.result = result
            job.status = "completed"
        except Exception as exc:  # noqa: BLE001
            if cancel.is_set() or _terminal(job.status):
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
