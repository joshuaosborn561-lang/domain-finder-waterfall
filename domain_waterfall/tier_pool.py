"""Bounded worker pool for tier row resolution.

Workers pull from a fixed in memory snapshot. They never re query the source table.
Progress counters are updated under a lock so the stall detector stays honest.
"""

from __future__ import annotations

import os
import queue
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from domain_waterfall.concurrency import VendorThrottle, VendorTransportError
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress

TIER_CONCURRENCY_CAP = 32
# Slice bench: concurrency 12 drops vendor hit rate about 5pp vs serial on this
# RapidAPI host. Concurrency 8 matches serial within 2pp and is the shipped default.
TIER_CONCURRENCY_DEFAULT = 8
ROW_ERROR_RETRIES = 3
WRITEBACK_CHUNK = 200

PER_TIER_ENV: dict[str, str] = {
    "maps": "MAPS_TIER_CONCURRENCY",
    "serp": "SERP_TIER_CONCURRENCY",
}

def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def resolve_tier_concurrency(tier: str, override: int | None = None) -> int:
    """Pick concurrency for a tier. Cap at 32."""
    if override is not None:
        try:
            n = int(override)
        except (TypeError, ValueError):
            n = TIER_CONCURRENCY_DEFAULT
        return max(1, min(TIER_CONCURRENCY_CAP, n))
    per = PER_TIER_ENV.get(tier)
    if per:
        val = _env_int(per, 0)
        if val > 0:
            return max(1, min(TIER_CONCURRENCY_CAP, val))
    return max(1, min(TIER_CONCURRENCY_CAP, _env_int("TIER_CONCURRENCY", TIER_CONCURRENCY_DEFAULT)))


@dataclass
class RowWorkResult:
    key: str
    candidate: DomainCandidate | None = None
    none: bool = False
    errored: bool = False
    requests: int = 0
    throttled: bool = False


ResolveOneFn = Callable[[dict[str, Any]], RowWorkResult]
StopFn = Callable[[], bool]


@dataclass
class ProgressTracker:
    """Race free counters for get_job_status."""

    total: int
    lock: threading.Lock = field(default_factory=threading.Lock)
    processed: int = 0
    rows_done: int = 0
    accepted: int = 0
    none: int = 0
    errored: int = 0
    requests_made: int = 0
    last_progress_at: float = field(default_factory=time.time)
    last_requests_made: int = 0
    last_requests_at: float = field(default_factory=time.time)

    def bump(
        self,
        *,
        processed: int = 0,
        rows_done: int = 0,
        accepted: int = 0,
        none: int = 0,
        errored: int = 0,
        requests: int = 0,
    ) -> dict[str, Any]:
        with self.lock:
            self.processed += processed
            self.rows_done += rows_done
            self.accepted += accepted
            self.none += none
            self.errored += errored
            if requests:
                self.requests_made += requests
                self.last_requests_made = self.requests_made
                self.last_requests_at = time.time()
            self.last_progress_at = time.time()
            return {
                "processed": self.processed,
                "targets": self.total,
                "hits": self.accepted,
                "rows_attempted": self.processed,
                "rows_done": self.rows_done,
                "accepted": self.accepted,
                "none": self.none,
                "errored": self.errored,
                "requests_made": self.requests_made,
            }

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "processed": self.processed,
                "targets": self.total,
                "hits": self.accepted,
                "rows_attempted": self.processed,
                "rows_done": self.rows_done,
                "accepted": self.accepted,
                "none": self.none,
                "errored": self.errored,
                "requests_made": self.requests_made,
                "last_requests_at": self.last_requests_at,
            }


class ThrottleController:
    """On 429, shrink the effective pool for a cooldown window."""

    def __init__(self, max_workers: int) -> None:
        self.max_workers = max(1, max_workers)
        self._lock = threading.Lock()
        self._cooldown_until = 0.0
        self._active = 0
        self._effective = self.max_workers
        self._cv = threading.Condition(self._lock)

    def note_throttle(self, seconds: float | None = None) -> None:
        delay = seconds if seconds is not None else 5.0 + random.uniform(0, 2)
        with self._cv:
            self._cooldown_until = max(self._cooldown_until, time.monotonic() + delay)
            self._effective = max(1, self._effective // 2)
            self._cv.notify_all()

    def note_ok(self) -> None:
        with self._cv:
            if time.monotonic() >= self._cooldown_until and self._effective < self.max_workers:
                self._effective = min(self.max_workers, self._effective + 1)

    def acquire(self, should_stop: StopFn | None = None) -> bool:
        with self._cv:
            while True:
                if should_stop and should_stop():
                    return False
                now = time.monotonic()
                if now < self._cooldown_until and self._active >= max(1, self._effective):
                    self._cv.wait(timeout=min(1.0, self._cooldown_until - now))
                    continue
                if self._active >= self._effective:
                    self._cv.wait(timeout=0.5)
                    continue
                self._active += 1
                return True

    def release(self) -> None:
        with self._cv:
            self._active = max(0, self._active - 1)
            self._cv.notify_all()


def _retry_sleep(attempt: int) -> None:
    time.sleep(min(30.0, (2**attempt) + random.uniform(0, 0.5)))


def run_row_pool(
    rows: list[dict[str, Any]],
    resolve_one: ResolveOneFn,
    *,
    tier: str,
    concurrency: int,
    on_progress: OnProgress | None = None,
    should_stop: StopFn | None = None,
    deadline: float | None = None,
    result: TierResult | None = None,
) -> TierResult:
    """Run resolve_one over a fixed row snapshot with bounded concurrency."""
    out = result or TierResult(tier=tier)
    total = len(rows)
    if total == 0:
        return out
    workers = max(1, min(TIER_CONCURRENCY_CAP, int(concurrency)))
    tracker = ProgressTracker(total=total)
    throttle = ThrottleController(workers)
    work_q: queue.Queue[dict[str, Any] | None] = queue.Queue()
    for row in rows:
        work_q.put(row)
    for _ in range(workers):
        work_q.put(None)

    emit_lock = threading.Lock()

    def emit_progress(extra: dict[str, Any]) -> None:
        with emit_lock:
            report_progress(
                on_progress,
                int(extra.get("processed") or 0),
                total,
                int(extra.get("accepted") or 0),
                extra,
            )

    def stopped() -> bool:
        if should_stop and should_stop():
            return True
        if deadline is not None and time.monotonic() >= deadline:
            return True
        return False

    def handle_row(row: dict[str, Any]) -> None:
        key = str(row.get("_source_key"))
        last_err: Exception | None = None
        requests_total = 0
        for attempt in range(ROW_ERROR_RETRIES):
            if stopped():
                return
            if not throttle.acquire(should_stop=stopped):
                return
            try:
                try:
                    work = resolve_one(row)
                except VendorThrottle as exc:
                    last_err = exc
                    throttle.note_throttle()
                    requests_total += 1
                    tracker.bump(requests=1)
                    emit_progress(tracker.snapshot())
                    _retry_sleep(attempt)
                    continue
                except (VendorTransportError, TimeoutError, OSError) as exc:
                    last_err = exc
                    requests_total += 1
                    tracker.bump(requests=1)
                    emit_progress(tracker.snapshot())
                    _retry_sleep(attempt)
                    continue
            finally:
                throttle.release()

            throttle.note_ok()
            requests_total += work.requests
            if work.throttled:
                throttle.note_throttle()
                tracker.bump(requests=work.requests)
                emit_progress(tracker.snapshot())
                _retry_sleep(attempt)
                continue

            if work.errored:
                last_err = VendorTransportError(tier, "row errored")
                tracker.bump(requests=work.requests)
                emit_progress(tracker.snapshot())
                _retry_sleep(attempt)
                continue

            with out_lock:
                if work.candidate is not None:
                    out.candidates[key] = work.candidate
                out.calls += work.requests
                out.billed_calls += work.requests
                if work.none:
                    out.none += 1
                out.rows_done += 1

            extra = tracker.bump(
                processed=1,
                rows_done=1,
                accepted=1 if work.candidate is not None else 0,
                none=1 if work.none else 0,
                requests=work.requests,
            )
            emit_progress(extra)
            return

        # Retries exhausted: record as errored, never silently as none.
        with out_lock:
            out.errored += 1
            out.rows_done += 1
            out.calls += max(1, requests_total)
            if last_err and not out.error:
                out.error = f"row errors after retries, e.g. {type(last_err).__name__}"
        extra = tracker.bump(
            processed=1,
            rows_done=1,
            errored=1,
            requests=max(1, requests_total),
        )
        emit_progress(extra)

    out_lock = threading.Lock()

    def worker() -> None:
        while True:
            if stopped():
                # Drain remaining markers without resolving more rows.
                try:
                    while True:
                        item = work_q.get_nowait()
                        work_q.task_done()
                        if item is None:
                            return
                except queue.Empty:
                    return
            try:
                item = work_q.get(timeout=0.5)
            except queue.Empty:
                if stopped():
                    return
                continue
            try:
                if item is None:
                    return
                if stopped():
                    return
                handle_row(item)
            finally:
                work_q.task_done()

    threads = [
        threading.Thread(target=worker, name=f"dw-{tier}-{i}", daemon=True)
        for i in range(workers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if stopped() and not out.error:
        if deadline is not None and time.monotonic() >= deadline:
            out.error = "tier timeout"
        elif should_stop and should_stop():
            out.error = "cancelled"

    snap = tracker.snapshot()
    # Align counters if workers exited early on cancel.
    unfinished = total - int(snap.get("processed") or 0)
    if unfinished > 0 and out.error:
        # Leave unfinished rows for later tiers; do not mark them none.
        pass
    emit_progress(tracker.snapshot())
    return out


def chunked(items: list[Any], size: int) -> list[list[Any]]:
    n = max(1, int(size))
    return [items[i : i + n] for i in range(0, len(items), n)]
