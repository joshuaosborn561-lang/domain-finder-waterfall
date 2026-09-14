"""Process-wide vendor semaphores. 429 backs off and retries; never disables a tier.

Acquire waits are bounded. The semaphore is never held across backoff sleep.
Each HTTP attempt also has a thread-level hard timeout so a stuck connect/DNS
cannot pin a tier forever — requests' own timeout does not cover getaddrinfo.
"""

from __future__ import annotations

import os
import random
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator, TypeVar

import requests


class VendorThrottle(Exception):
    """Vendor returned 429 or equivalent. Not a miss, not credit exhaustion."""

    def __init__(self, tier: str, detail: str = "") -> None:
        msg = f"vendor throttle: {tier}"
        if detail:
            msg = f"{msg}, {detail}"
        super().__init__(msg)
        self.tier = tier


class VendorTransportError(Exception):
    """Transport or server failure. Retryable. Must not become a miss."""

    def __init__(self, tier: str, detail: str = "") -> None:
        msg = f"vendor transport error: {tier}"
        if detail:
            msg = f"{msg}, {detail}"
        super().__init__(msg)
        self.tier = tier

TIER_ENV_KEYS: dict[str, str] = {
    "maps": "MAPS_CONCURRENCY",
    "aiark": "AIARK_CONCURRENCY",
    "discolike": "DISCOLIKE_CONCURRENCY",
    "serp": "SERP_CONCURRENCY",
    "prospeo": "PROSPEO_CONCURRENCY",
    "leadmagic": "LEADMAGIC_CONCURRENCY",
}

DEFAULT_VENDOR_LIMITS: dict[str, int] = {
    # Maps HTTP slots must cover TIER_CONCURRENCY (default 12), else the pool stalls on the gate.
    "maps": 16,
    "aiark": 8,
    "discolike": 2,
    "serp": 8,
    "prospeo": 6,
    "leadmagic": 4,
    "cache": 20,
}

DEFAULT_ACQUIRE_TIMEOUT_S = 10.0
MAPS_MAX_ATTEMPTS = 1

T = TypeVar("T")


class VendorAcquireTimeout(TimeoutError):
    """Could not take a vendor slot before acquire_timeout."""

    def __init__(self, tier: str, timeout: float) -> None:
        super().__init__(f"vendor lock timeout: {tier} after {timeout:.1f}s")
        self.tier = tier
        self.timeout = timeout


class VendorCallTimeout(TimeoutError):
    """Outbound call exceeded its hard deadline (or requests timed out)."""

    def __init__(self, tier: str, detail: str = "") -> None:
        msg = f"vendor call timeout: {tier}"
        if detail:
            msg = f"{msg}: {detail}"
        super().__init__(msg)
        self.tier = tier


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


def timeout_budget_s(timeout: object | None) -> float:
    if timeout is None:
        return 15.0
    if isinstance(timeout, (tuple, list)) and len(timeout) >= 2:
        return max(0.1, float(timeout[0]) + float(timeout[1]))
    try:
        return max(0.1, float(timeout))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 15.0


def run_with_timeout(fn: Callable[[], T], seconds: float) -> T:
    """Run fn in a daemon thread; raise VendorCallTimeout if it exceeds seconds."""
    box: dict[str, Any] = {}
    done = threading.Event()

    def _run() -> None:
        try:
            box["v"] = fn()
        except Exception as exc:  # noqa: BLE001
            box["e"] = exc
        finally:
            done.set()

    threading.Thread(target=_run, name="dw-http", daemon=True).start()
    if not done.wait(seconds):
        raise VendorCallTimeout("http", f"exceeded {seconds:.1f}s")
    if "e" in box:
        raise box["e"]
    return box["v"]


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
    def acquire(self, tier: str, timeout: float | None = DEFAULT_ACQUIRE_TIMEOUT_S) -> Iterator[None]:
        sem = self._sem(tier)
        if timeout is None or timeout <= 0:
            acquired = sem.acquire()
        else:
            acquired = sem.acquire(timeout=timeout)
        if not acquired:
            raise VendorAcquireTimeout(tier, float(timeout or 0.0))
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
    max_attempts: int | None = None,
    acquire_timeout: float | None = DEFAULT_ACQUIRE_TIMEOUT_S,
    **kwargs: object,
) -> requests.Response | None:
    if max_attempts is None:
        max_attempts = MAPS_MAX_ATTEMPTS if tier == "maps" else 4
    last: requests.Response | None = None
    timeout = kwargs.get("timeout", 45)
    hard_s = timeout_budget_s(timeout) + 1.0
    for attempt in range(max_attempts):
        try:
            with vendor_gate.acquire(tier, timeout=acquire_timeout):
                try:
                    last = run_with_timeout(
                        lambda: requests.request(method, url, **kwargs),  # type: ignore[arg-type]
                        hard_s,
                    )
                except VendorCallTimeout as exc:
                    exc.tier = tier
                    raise VendorCallTimeout(tier, str(exc)) from exc
        except VendorAcquireTimeout:
            raise
        except VendorCallTimeout:
            if attempt < max_attempts - 1:
                time.sleep(_retry_delay(attempt, None))
                continue
            raise
        except requests.Timeout as exc:
            if attempt < max_attempts - 1:
                time.sleep(_retry_delay(attempt, None))
                continue
            raise VendorCallTimeout(tier, str(exc)) from exc
        except requests.RequestException as exc:
            if attempt < max_attempts - 1:
                time.sleep(_retry_delay(attempt, None))
                continue
            raise VendorTransportError(tier, str(exc)) from exc
        if last is not None and last.status_code == 429:
            if attempt < max_attempts - 1:
                time.sleep(_retry_delay(attempt, last))
                continue
            raise VendorThrottle(tier, "http 429")
        if last is not None and last.status_code >= 500:
            if attempt < max_attempts - 1:
                time.sleep(_retry_delay(attempt, last))
                continue
            raise VendorTransportError(tier, f"http {last.status_code}")
        return last
    return last
