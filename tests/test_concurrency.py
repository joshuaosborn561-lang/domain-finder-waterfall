import threading
import time
from unittest.mock import MagicMock

import pytest
import requests

from domain_waterfall import concurrency
from domain_waterfall.concurrency import (
    VendorAcquireTimeout,
    VendorCallTimeout,
    request_with_retry,
    run_with_timeout,
    timeout_budget_s,
    vendor_gate,
)


def test_timeout_budget_tuple() -> None:
    assert timeout_budget_s((5, 15)) == 20
    assert timeout_budget_s(15) == 15


def test_run_with_timeout_raises() -> None:
    def hang() -> None:
        time.sleep(2)

    t0 = time.monotonic()
    with pytest.raises(VendorCallTimeout):
        run_with_timeout(hang, 0.15)
    assert time.monotonic() - t0 < 1.0


def test_acquire_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAPS_CONCURRENCY", "1")
    vendor_gate.reset()
    held = threading.Event()
    release = threading.Event()

    def holder() -> None:
        with vendor_gate.acquire("maps", timeout=1):
            held.set()
            release.wait(3)

    t = threading.Thread(target=holder)
    t.start()
    assert held.wait(1)
    with pytest.raises(VendorAcquireTimeout):
        with vendor_gate.acquire("maps", timeout=0.15):
            pass
    release.set()
    t.join(2)
    vendor_gate.reset()


def test_semaphore_released_during_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MAPS_CONCURRENCY", "1")
    vendor_gate.reset()
    in_sleep = threading.Event()
    acquired_during_sleep = threading.Event()

    def delay(_attempt: int, _response: object) -> float:
        in_sleep.set()
        time.sleep(0.35)
        return 0.0

    def fake_request(*_a: object, **_k: object) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 429
        resp.headers = {}
        return resp

    monkeypatch.setattr(concurrency, "_retry_delay", delay)
    monkeypatch.setattr(requests, "request", fake_request)

    def worker() -> None:
        request_with_retry("maps", "GET", "http://example.invalid", max_attempts=2, timeout=1)

    t = threading.Thread(target=worker)
    t.start()
    assert in_sleep.wait(1)
    with vendor_gate.acquire("maps", timeout=0.2):
        acquired_during_sleep.set()
    t.join(2)
    vendor_gate.reset()
    assert acquired_during_sleep.is_set()


def test_hanging_http_hits_hard_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def hang(*_a: object, **_k: object) -> None:
        time.sleep(30)

    monkeypatch.setattr(requests, "request", hang)
    t0 = time.monotonic()
    with pytest.raises(VendorCallTimeout):
        request_with_retry(
            "maps",
            "GET",
            "http://example.invalid",
            timeout=0.2,
            max_attempts=1,
            acquire_timeout=1,
        )
    assert time.monotonic() - t0 < 3.0
    vendor_gate.reset()
