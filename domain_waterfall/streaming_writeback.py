"""Flush settled writeback rows while vendor calls are still in flight.

Workers only enqueue. A writer thread posts to dw_patch_source_batch whenever
500 rows are queued or WRITEBACK_FLUSH_INTERVAL_S seconds have passed.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from .source import (
    WRITEBACK_BATCH,
    WritebackResult,
)

log = logging.getLogger("domain_waterfall.writeback")

WRITEBACK_FLUSH_INTERVAL_S = 60.0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resolve_flush_interval(override: float | None = None) -> float:
    if override is not None:
        try:
            return max(0.01, float(override))
        except (TypeError, ValueError):
            return WRITEBACK_FLUSH_INTERVAL_S
    raw = (os.environ.get("WRITEBACK_FLUSH_INTERVAL_S") or "").strip()
    if not raw:
        return WRITEBACK_FLUSH_INTERVAL_S
    try:
        return max(0.01, float(raw))
    except ValueError:
        return WRITEBACK_FLUSH_INTERVAL_S


FlushFn = Callable[[list[str], str], WritebackResult | list[Any] | None]


class StreamingWriteback:
    """Queue of settled source keys. Flush on chunk size or a timer, not at tier end."""

    def __init__(
        self,
        *,
        flush_keys: FlushFn,
        chunk: int = WRITEBACK_BATCH,
        interval_s: float | None = None,
        clock: Callable[[], float] = time.monotonic,
        enabled: bool = True,
    ) -> None:
        self.flush_keys = flush_keys
        self.chunk = max(1, int(chunk))
        self.interval_s = resolve_flush_interval(interval_s)
        self.clock = clock
        self.enabled = enabled
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._pending: list[str] = []
        self._pending_set: set[str] = set()
        self._saved: set[str] = set()
        self._last_flush_mono = clock()
        self.last_flush_at: str | None = None
        self._stopped = False
        self._thread: threading.Thread | None = None

    @property
    def saved_rows(self) -> int:
        with self._lock:
            return len(self._saved)

    @property
    def saved_keys(self) -> set[str]:
        with self._lock:
            return set(self._saved)

    @property
    def queued(self) -> int:
        with self._lock:
            return len(self._pending)

    def start(self) -> None:
        if not self.enabled or self._thread is not None:
            return
        self._stopped = False
        self._last_flush_mono = self.clock()
        self._thread = threading.Thread(
            target=self._run, name="dw-writeback-flush", daemon=True
        )
        self._thread.start()

    def enqueue(self, key: str) -> None:
        if not self.enabled:
            return
        text = str(key)
        if not text:
            return
        with self._cv:
            if self._stopped:
                return
            if text not in self._pending_set:
                self._pending.append(text)
                self._pending_set.add(text)
            if len(self._pending) >= self.chunk:
                self._cv.notify()

    def enqueue_many(self, keys: list[str]) -> None:
        for key in keys:
            self.enqueue(key)

    def _take_locked(self, n: int | None = None) -> list[str]:
        if not self._pending:
            return []
        take = len(self._pending) if n is None else min(n, len(self._pending))
        batch = self._pending[:take]
        del self._pending[:take]
        for key in batch:
            self._pending_set.discard(key)
        return batch

    def _run(self) -> None:
        while True:
            with self._cv:
                if self._stopped and not self._pending:
                    return
                while not self._stopped and len(self._pending) < self.chunk:
                    now = self.clock()
                    due = self._last_flush_mono + self.interval_s
                    if self._pending and now >= due:
                        break
                    if self._pending:
                        timeout = max(0.01, min(0.25, due - now))
                    else:
                        timeout = 0.25
                    self._cv.wait(timeout=timeout)
                if not self._pending:
                    if self._stopped:
                        return
                    continue
                if len(self._pending) >= self.chunk and not self._stopped:
                    batch = self._take_locked(self.chunk)
                    phase = "writeback"
                else:
                    batch = self._take_locked()
                    phase = "writeback_flush" if self._stopped else "writeback"
            if batch:
                self._do_flush(batch, phase=phase)

    def _do_flush(self, keys: list[str], *, phase: str) -> WritebackResult:
        if not keys:
            return WritebackResult()
        try:
            raw = self.flush_keys(keys, phase)
        except Exception as exc:  # noqa: BLE001
            log.exception("streaming writeback flush failed, rows=%s, %s", len(keys), exc)
            with self._lock:
                self._last_flush_mono = self.clock()
                self.last_flush_at = utc_now_iso()
            return WritebackResult(failed_items=[(k, {}) for k in keys])
        if not isinstance(raw, WritebackResult):
            raw = WritebackResult(written_keys=list(raw or []))
        with self._lock:
            for key in raw.written_keys:
                self._saved.add(str(key))
            self._last_flush_mono = self.clock()
            self.last_flush_at = utc_now_iso()
        return raw

    def flush_all(self, *, phase: str = "writeback_flush") -> WritebackResult:
        """Drain the queue on the caller thread (end of tier / job)."""
        if not self.enabled:
            return WritebackResult()
        with self._cv:
            batch = self._take_locked()
        if not batch:
            return WritebackResult()
        return self._do_flush(batch, phase=phase)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "saved_rows": len(self._saved),
                "last_flush_at": self.last_flush_at,
                "queued_rows": len(self._pending),
            }

    def close(self) -> None:
        if not self.enabled:
            return
        with self._cv:
            self._stopped = True
            self._cv.notify_all()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=60.0)
        self._thread = None
        self.flush_all(phase="writeback_flush")
