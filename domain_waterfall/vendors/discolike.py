"""DiscoLike bulk-match-company-to-domain. Always pass location inputs when present.

Chunks run sequentially (never as concurrent DiscoLike tasks). Our gate is the
accuracy check; vendor min_match_confidence / strict come from the profile.
Each finished chunk notifies the streaming writeback via on_settled.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import time
from typing import Any, Callable

from domain_waterfall import http_client
from domain_waterfall.config import settings
from domain_waterfall.normalize import extract_domain
from domain_waterfall.tier_pool import chunked
from domain_waterfall.vendors.base import (
    DomainCandidate,
    OnProgress,
    OnSettled,
    TierResult,
    report_progress,
)

log = logging.getLogger("domain_waterfall.discolike")

BASE = "https://api.discolike.com/v1"
UNIT = 0.00425
DEFAULT_CHUNK = 500
MAX_CHUNK = 1000
POLL_MIN_S = 2.0
POLL_MAX_S = 15.0
CHUNK_TIMEOUT_S = 15 * 60
DEFAULT_MIN_CONFIDENCE = 60
DEFAULT_STRICT = False

SleepFn = Callable[[float], None]
ClockFn = Callable[[], float]


def _headers() -> dict[str, str]:
    return {"x-discolike-key": settings.discolike_api_key, "Accept": "application/json"}


def _int_or(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def resolve_chunk_size(
    override: int | None = None,
    profile: Any | None = None,
) -> int:
    if override is not None:
        n = _int_or(override, DEFAULT_CHUNK)
    elif profile is not None and getattr(profile, "discolike_chunk_size", None):
        n = _int_or(profile.discolike_chunk_size, DEFAULT_CHUNK)
    else:
        raw = (os.environ.get("DISCOLIKE_CHUNK_SIZE") or "").strip()
        n = _int_or(raw, DEFAULT_CHUNK) if raw else DEFAULT_CHUNK
    return max(1, min(MAX_CHUNK, n))


def _min_confidence(profile: Any | None) -> int:
    if profile is not None and hasattr(profile, "discolike_min_confidence"):
        return max(0, min(100, _int_or(profile.discolike_min_confidence, DEFAULT_MIN_CONFIDENCE)))
    if profile is not None:
        raw = getattr(profile, "raw", None)
        if isinstance(raw, dict) and "discolike_min_confidence" in raw:
            return max(0, min(100, _int_or(raw.get("discolike_min_confidence"), DEFAULT_MIN_CONFIDENCE)))
    return DEFAULT_MIN_CONFIDENCE


def _strict(profile: Any | None) -> bool:
    if profile is not None and hasattr(profile, "discolike_strict"):
        return bool(profile.discolike_strict)
    if profile is not None:
        raw = getattr(profile, "raw", None)
        if isinstance(raw, dict) and "discolike_strict" in raw:
            return bool(raw.get("discolike_strict"))
    return DEFAULT_STRICT


def _status_billed(body: dict[str, Any], results: list[dict[str, Any]]) -> int:
    """Actual billed count: status field if present, else results without match_error."""
    for key in (
        "billed",
        "billed_count",
        "billed_rows",
        "num_billed",
        "charged",
        "credits_used",
        "credits",
    ):
        val = body.get(key)
        if isinstance(val, bool) or val in (None, ""):
            continue
        try:
            return max(0, int(val))
        except (TypeError, ValueError):
            continue
    for nest_key in ("billing", "usage", "stats"):
        nest = body.get(nest_key)
        if not isinstance(nest, dict):
            continue
        for key in ("billed", "billed_count", "count", "credits", "credits_used"):
            val = nest.get(key)
            if isinstance(val, bool) or val in (None, ""):
                continue
            try:
                return max(0, int(val))
            except (TypeError, ValueError):
                continue
    return sum(1 for item in results if not item.get("match_error"))


def _csv_payload(rows: list[dict[str, Any]], *, with_location: bool) -> bytes:
    buf = io.StringIO()
    fieldnames = ["row_key", "name", "city", "state", "country", "phone", "zip"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {
                "row_key": str(row.get("_source_key") or ""),
                "name": str(row.get("company_name") or ""),
                "city": str(row.get("city") or "") if with_location else "",
                "state": str(row.get("state") or "") if with_location else "",
                "country": str(row.get("country") or "US") if with_location else "",
                "phone": str(row.get("phone") or "") if with_location else "",
                "zip": str(row.get("zip") or "") if with_location else "",
            }
        )
    return buf.getvalue().encode("utf-8")


def _chunk_keys(rows: list[dict[str, Any]]) -> list[str]:
    return [str(row.get("_source_key") or "") for row in rows]


def _mark_deferred(result: TierResult, keys: list[str], error: str) -> None:
    result.error = result.error or error
    for key in keys:
        if key and key not in result.deferred_keys:
            result.deferred_keys.append(key)
    result.errored += len([k for k in keys if k])


def _notify_settled(
    on_settled: OnSettled | None,
    key: str,
    candidate: DomainCandidate | None,
    *,
    deferred: bool = False,
) -> None:
    if not on_settled or not key:
        return
    try:
        if deferred:
            on_settled(key, None, deferred=True)
        else:
            on_settled(key, candidate)
    except TypeError:
        on_settled(key, candidate)


def _ingest_results(
    result: TierResult,
    items: list[dict[str, Any]],
    *,
    inputs: list[str],
    unit: float,
) -> set[str]:
    seen: set[str] = set()
    for item in items:
        key = str(item.get("input:row_key") or item.get("row_key") or "")
        domain = extract_domain(str(item.get("domain") or item.get("website") or ""))
        if item.get("match_error"):
            continue
        if not key or not domain:
            continue
        phones = item.get("phones") or item.get("phone")
        phone = ""
        if isinstance(phones, list) and phones:
            phone = str(phones[0] if not isinstance(phones[0], dict) else phones[0].get("number") or "")
        elif isinstance(phones, str):
            phone = phones
        addr = item.get("address") if isinstance(item.get("address"), dict) else {}
        result.candidates[key] = DomainCandidate(
            domain=domain,
            vendor_name=str(item.get("name") or ""),
            phone=phone,
            address_state=str(addr.get("state") or item.get("state") or ""),
            address_city=str(addr.get("city") or item.get("city") or ""),
            raw={"match_confidence": item.get("match_confidence")},
            inputs_passed=inputs,
            billed=True,
            cost_usd=unit,
        )
        seen.add(key)
    return seen


def _poll_task(
    task_id: str,
    *,
    timeout_s: float,
    sleep: SleepFn,
    clock: ClockFn,
) -> tuple[str, dict[str, Any] | None]:
    """Poll until completed/failed or timeout. Backoff 2s → 15s. Sequential only."""
    delay = POLL_MIN_S
    deadline = clock() + timeout_s
    while True:
        st = http_client.get(
            "discolike",
            f"{BASE}/bulkmatch/status/{task_id}",
            headers=_headers(),
            timeout=30,
        )
        body: dict[str, Any] | None = None
        status = ""
        if st is not None:
            try:
                raw = st.json()
            except ValueError:
                raw = None
            if isinstance(raw, dict):
                body = raw
                status = str(body.get("status") or "")
        if status == "completed":
            return "completed", body or {}
        if status == "failed":
            return "failed", body or {}
        if clock() >= deadline:
            return "timeout", body
        sleep(delay)
        delay = min(POLL_MAX_S, max(POLL_MIN_S, delay * 2))


def _settle_chunk(
    on_settled: OnSettled | None,
    keys: list[str],
    result: TierResult,
    *,
    deferred: bool,
) -> None:
    deferred_set = set(result.deferred_keys) if deferred else set()
    for key in keys:
        if not key:
            continue
        if key in deferred_set:
            # Timeout / failed: write deferred, never a silent miss.
            _notify_settled(on_settled, key, None, deferred=True)
            continue
        cand = result.candidates.get(key)
        if cand:
            _notify_settled(on_settled, key, cand)
        else:
            _notify_settled(on_settled, key, None)


def _run_chunk(
    chunk: list[dict[str, Any]],
    result: TierResult,
    *,
    with_location: bool,
    params: dict[str, str],
    inputs: list[str],
    unit: float,
    poll_timeout_s: float,
    sleep: SleepFn,
    clock: ClockFn,
    on_settled: OnSettled | None,
) -> None:
    keys = [k for k in _chunk_keys(chunk) if k]
    payload = _csv_payload(chunk, with_location=with_location)
    r = http_client.post(
        "discolike",
        f"{BASE}/bulkmatch",
        params=params,
        files={"file": ("companies.csv", payload, "text/csv")},
        headers=_headers(),
        timeout=60,
    )
    result.calls += 1
    if r is None or r.status_code >= 400:
        _mark_deferred(result, keys, f"bulkmatch_http_{getattr(r, 'status_code', 0)}")
        _settle_chunk(on_settled, keys, result, deferred=True)
        return
    try:
        started = r.json()
    except ValueError:
        _mark_deferred(result, keys, "bulkmatch_bad_json")
        _settle_chunk(on_settled, keys, result, deferred=True)
        return
    if not isinstance(started, dict):
        _mark_deferred(result, keys, "bulkmatch_bad_json")
        _settle_chunk(on_settled, keys, result, deferred=True)
        return
    task_id = str(started.get("task_id") or "")
    if not task_id:
        _mark_deferred(result, keys, "bulkmatch_no_task")
        _settle_chunk(on_settled, keys, result, deferred=True)
        return

    status, body = _poll_task(
        task_id, timeout_s=poll_timeout_s, sleep=sleep, clock=clock
    )
    if status == "timeout":
        _mark_deferred(result, keys, "bulkmatch_timeout")
        _settle_chunk(on_settled, keys, result, deferred=True)
        return
    if status == "failed":
        _mark_deferred(result, keys, "bulkmatch_failed")
        _settle_chunk(on_settled, keys, result, deferred=True)
        return

    body = body or {}
    raw = body.get("results") or []
    items = [x for x in raw if isinstance(x, dict)]
    billed = _status_billed(body, items)
    result.billed_calls += billed
    result.cost_usd += billed * unit
    _ingest_results(result, items, inputs=inputs, unit=unit)
    seen = set(result.candidates)
    misses = [k for k in keys if k not in seen and k not in result.deferred_keys]
    result.none += len(misses)
    result.rows_done += len(keys)
    _settle_chunk(on_settled, keys, result, deferred=False)


def resolve_rows(
    rows: list[dict[str, Any]],
    *,
    with_location: bool = True,
    on_progress: OnProgress | None = None,
    on_settled: OnSettled | None = None,
    profile: Any | None = None,
    chunk_size: int | None = None,
    poll_timeout_s: float | None = None,
    sleep: SleepFn = time.sleep,
    clock: ClockFn = time.monotonic,
) -> TierResult:
    inputs = ["company_name"]
    if with_location:
        inputs.extend(["city", "state", "country", "phone"])
    result = TierResult(tier="discolike", inputs_passed=inputs)
    if not settings.discolike_api_key:
        result.skipped = "discolike_key_missing"
        report_progress(on_progress, 0, len(rows), 0)
        return result
    if not rows:
        report_progress(on_progress, 0, 0, 0)
        return result
    report_progress(on_progress, 0, len(rows), 0)

    size = resolve_chunk_size(chunk_size, profile)
    timeout_s = float(poll_timeout_s) if poll_timeout_s is not None else float(CHUNK_TIMEOUT_S)
    min_conf = _min_confidence(profile)
    strict = _strict(profile)
    params = {
        "name_column": "name",
        "min_match_confidence": str(min_conf),
        "strict": "true" if strict else "false",
    }
    if with_location:
        params.update(
            {
                "city_column": "city",
                "state_column": "state",
                "country_column": "country",
                "phone_column": "phone",
                "zip_code_column": "zip",
            }
        )

    processed = 0
    for chunk in chunked(rows, size):
        _run_chunk(
            chunk,
            result,
            with_location=with_location,
            params=params,
            inputs=inputs,
            unit=UNIT,
            poll_timeout_s=timeout_s,
            sleep=sleep,
            clock=clock,
            on_settled=on_settled,
        )
        processed += len(chunk)
        report_progress(on_progress, processed, len(rows), len(result.candidates))
        log.info(
            "discolike chunk done processed=%s/%s hits=%s deferred=%s error=%s cost=%s",
            processed,
            len(rows),
            len(result.candidates),
            len(result.deferred_keys),
            result.error,
            round(result.cost_usd, 6),
        )

    if result.error and not result.candidates and not result.deferred_keys:
        result.errored = max(result.errored, len(rows))
    return result
