import time

import pytest

from mcp_server import jobs


def test_stall_marks_failed_and_is_sticky(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setattr(jobs, "STALL_SECONDS", 0.2)
    monkeypatch.setattr(jobs, "STALL_POLL_SECONDS", 0.05)
    monkeypatch.setattr(jobs, "JOBS_DIR", tmp_path)

    def slow(_job: jobs.Job) -> dict:
        time.sleep(1.4)
        return {"ok": True, "status": "completed"}

    job = jobs.start_job("stall-test", slow)
    deadline = time.time() + 2.0
    payload = jobs.get_job(job.id)
    while time.time() < deadline:
        payload = jobs.get_job(job.id)
        if payload["status"] == "failed":
            break
        time.sleep(0.05)
    assert payload["status"] == "failed"
    assert "stalled" in (payload.get("error") or "")
    time.sleep(1.3)
    later = jobs.get_job(job.id)
    assert later["status"] == "failed"
    assert later.get("error") and "stalled" in later["error"]
