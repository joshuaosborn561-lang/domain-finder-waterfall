import time

import pytest

from mcp_server import jobs


def test_stall_on_flat_requests_made(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(jobs, "STALL_SECONDS", 0.25)
    monkeypatch.setattr(jobs, "STALL_POLL_SECONDS", 0.05)
    monkeypatch.setattr(jobs, "JOBS_DIR", tmp_path)

    def slow(job: jobs.Job) -> dict:
        # Flat requests_made while claiming to run maps.
        jobs.update_job_progress(
            job.id,
            {
                "phase": "tier",
                "tier": "maps",
                "requests_made": 10,
                "processed": 5,
                "status": "running",
            },
        )
        time.sleep(1.2)
        return {"ok": True, "status": "completed", "tiers": []}

    job = jobs.start_job("stall-test", slow)
    deadline = time.time() + 2.5
    payload = jobs.get_job(job.id)
    while time.time() < deadline:
        payload = jobs.get_job(job.id)
        if payload["status"] == "stalled":
            break
        time.sleep(0.05)
    assert payload["status"] == "stalled"
    assert "requests_made" in (payload.get("error") or "")
    assert "maps" in (payload.get("error") or "")
    time.sleep(1.0)
    later = jobs.get_job(job.id)
    assert later["status"] == "stalled"


def test_last_progress_at_heartbeat_is_not_a_stall(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setattr(jobs, "STALL_SECONDS", 0.25)
    monkeypatch.setattr(jobs, "STALL_POLL_SECONDS", 0.05)
    monkeypatch.setattr(jobs, "JOBS_DIR", tmp_path)

    def beating(job: jobs.Job) -> dict:
        end = time.time() + 0.8
        n = 0
        while time.time() < end:
            n += 1
            jobs.update_job_progress(
                job.id,
                {
                    "phase": "tier",
                    "tier": "serp",
                    "requests_made": 2,
                    "processed": 0,
                    "status": "running",
                    "last_progress_at": f"2026-09-22T14:43:{n:02d}Z",
                    "serp_run_ids": ["cSSnS3yzvUDmTcZIe"],
                },
            )
            time.sleep(0.05)
        return {"ok": True, "status": "completed", "tiers": [], "spent_usd": 0.9}

    job = jobs.start_job("heartbeat-test", beating)
    deadline = time.time() + 2.0
    payload = jobs.get_job(job.id)
    while time.time() < deadline:
        payload = jobs.get_job(job.id)
        if payload["status"] in {"completed", "stalled", "failed"}:
            break
        time.sleep(0.05)
    assert payload["status"] == "completed"
    assert payload["status"] != "stalled"


def test_cancel_job_is_sticky(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    monkeypatch.setattr(jobs, "JOBS_DIR", tmp_path)
    monkeypatch.setattr(jobs, "STALL_SECONDS", 60)
    monkeypatch.setattr(jobs, "STALL_POLL_SECONDS", 30)

    started = time.time()

    def slow(job: jobs.Job) -> dict:
        while not jobs.is_cancelled(job.id):
            jobs.update_job_progress(
                job.id,
                {
                    "phase": "tier",
                    "tier": "maps",
                    "requests_made": 3,
                    "processed": 1,
                    "status": "running",
                },
            )
            time.sleep(0.05)
        return {"ok": False, "tiers": [{"tier": "maps", "accepted": 0}], "partial": True}

    job = jobs.start_job("cancel-test", slow)
    time.sleep(0.1)
    out = jobs.request_cancel(job.id, "cancelled by caller", status="cancelled")
    assert out["status"] == "cancelled"
    assert time.time() - started < 30
    deadline = time.time() + 2
    while time.time() < deadline:
        if jobs.get_job(job.id)["status"] == "cancelled":
            break
        time.sleep(0.05)
    assert jobs.get_job(job.id)["status"] == "cancelled"
