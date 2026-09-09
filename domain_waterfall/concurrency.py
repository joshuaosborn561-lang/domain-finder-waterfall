"""Process-wide vendor semaphores. 429 backs off and retries; never disables a tier."""

from __future__ import annotations

import os
import random
import threading
import time
from contextlib import contextmanager
from typing import Iterator

import requests

TIER_ENV_KEYS: dict[str, str] = {
    "maps": "MAPS_CONCURRENCY",
    "aiark": "AIARK_CONCURRENCY",
    "discolike": "DISCOLIKE_CONCURRENCY",
    "serp": "SERP_CONCURRENCY",
    "prospeo": "PROSPEO_CONCURRENCY",
    "leadmagic": "LEADMAGIC_CONCURRENCY",
}

DEFAULT_VENDOR_LIMITS: dict[str, int] = {
    "maps": 4,
    "aiark": 8,
    "discolike": 2,
    "serp": 2,
    "prospeo": 6,
    "leadmagic": 4,
    "cache": 20,
}


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def vendor_concurrency(tier: str) -> int:
    key = TIER_ENV_KEYS.get(tier)
    default = DEFAULT_VENDOR_LIMITS.get(tier, 4)
    return _env_int(key, default) if key else default


class VendorGate:
    def __init__(self) -> None:
        self._sems: dict[str, threading.Semaphore] = {}
        self._lock = threading.Lock()

    def reset(self) -> None:
        with self._lock:
            self._sems.clear()

    def _sem(self, tier: str) -> threading.Semaphore:
        with self._lock:
            if tier not in self._sems:
                self._sems[tier] = threading.Semaphore(vendor_concurrency(tier))
            return self._sems[tier]

    @contextmanager
    def acquire(self, tier: str) -> Iterator[None]:
        sem = self._sem(tier)
        sem.acquire()
        try:
            yield
        finally:
            sem.release()


vendor_gate = VendorGate()


def _retry_delay(attempt: int, response: requests.Response | None) -> float:
    if response is not None:
        retry_after = (response.headers.get("Retry-After") or "").strip()
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except ValueError:
                pass
        if response.status_code == 429:
            return min(60.0, 5 * (2**attempt)) + random.uniform(0, 0.5)
    return (2**attempt) + random.uniform(0, 0.25)


def request_with_retry(
    tier: str,
    method: str,
    url: str,
    *,
    max_attempts: int = 4,
    **kwargs: object,
) -> requests.Response | None:
    last: requests.Response | None = None
    with vendor_gate.acquire(tier):
        for attempt in range(max_attempts):
            try:
                last = requests.request(method, url, **kwargs)  # type: ignore[arg-type]
            except requests.RequestException:
                if attempt < max_attempts - 1:
                    time.sleep(_retry_delay(attempt, None))
                    continue
                return None
            if last.status_code == 429 or last.status_code >= 500:
                if attempt < max_attempts - 1:
                    time.sleep(_retry_delay(attempt, last))
                    continue
            return last
    return last
