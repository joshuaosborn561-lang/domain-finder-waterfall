"""Maps resolve_places via RapidAPI Maps Data. Score is ignored; ranking is used."""

from __future__ import annotations

import time
from typing import Any, Callable
from urllib.parse import urlencode

from domain_waterfall import http_client
from domain_waterfall.concurrency import VendorAcquireTimeout, VendorCallTimeout
from domain_waterfall.config import settings
from domain_waterfall.normalize import extract_domain
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress

MAPS_CALL_TIMEOUT_S = 15
MAPS_MAX_ATTEMPTS = 1
CONSECUTIVE_TIMEOUT_LIMIT = 3
SKIP_NOT_CONFIGURED = "not configured"
SKIP_UNRESPONSIVE = "vendor unresponsive"

StopFn = Callable[[], bool]


def _headers() -> dict[str, str]:
    return {
        "x-rapidapi-key": settings.rapidapi_key,
        "x-rapidapi-host": settings.maps_host,
        "Accept": "application/json",
    }


def _maps_get(url: str) -> Any:
    return http_client.get(
        "maps",
        url,
        headers=_headers(),
        timeout=MAPS_CALL_TIMEOUT_S,
        max_attempts=MAPS_MAX_ATTEMPTS,
    )


def _search(query: str) -> list[dict[str, Any]]:
    params = {
        "query": query,
        "limit": "8",
        "country": "us",
        "lang": "en",
        "offset": "0",
        "zoom": "13",
    }
    url = f"https://{settings.maps_host}/searchmaps.php?{urlencode(params)}"
    r = _maps_get(url)
    if r is None or r.status_code >= 400:
        return []
    try:
        payload = r.json()
    except ValueError:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("data", "results", "items"):
        val = payload.get(key)
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]
        if isinstance(val, dict) and isinstance(val.get("items"), list):
            return [x for x in val["items"] if isinstance(x, dict)]
    return []


def _details(place_id: str) -> dict[str, Any]:
    pid = (place_id or "").strip()
    if not pid:
        return {}
    params = {"lang": "en", "country": "us"}
    if pid.startswith("0x") or (":" in pid and not pid.startswith("Ch")):
        params["business_id"] = pid
    else:
        params["place_id"] = pid
    url = f"https://{settings.maps_host}/place.php?{urlencode(params)}"
    r = _maps_get(url)
    if r is None or r.status_code >= 400:
        return {}
    try:
        payload = r.json()
    except ValueError:
        return {}
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return data[0]
        return payload
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return payload[0]
    return {}


def _domain_from_value(val: Any) -> str:
    if isinstance(val, str) and val.strip():
        return extract_domain(val)
    if isinstance(val, dict):
        for key in ("url", "website", "href", "value", "uri", "link", "domain", "site"):
            found = _domain_from_value(val.get(key))
            if found:
                return found
    if isinstance(val, list):
        for item in val:
            found = _domain_from_value(item)
            if found:
                return found
    return ""


def _pick_domain(row: dict[str, Any]) -> str:
    for key in ("website", "domain", "site", "url", "website_uri", "websiteUri"):
        found = _domain_from_value(row.get(key))
        if found:
            return found
    for key in ("urls", "links", "web"):
        found = _domain_from_value(row.get(key))
        if found:
            return found
    return ""


def _pick_phone(row: dict[str, Any]) -> str:
    for key in ("phone", "phone_number", "international_phone"):
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    phones = row.get("phones") or row.get("phone_numbers")
    if isinstance(phones, list) and phones:
        first = phones[0]
        if isinstance(first, str):
            return first
        if isinstance(first, dict):
            return str(first.get("number") or first.get("phone") or "")
    return ""


def _expired(deadline: float | None, should_stop: StopFn | None) -> str | None:
    if should_stop and should_stop():
        if deadline is not None and time.monotonic() >= deadline:
            return "tier timeout"
        return "cancelled"
    if deadline is not None and time.monotonic() >= deadline:
        return "tier timeout"
    return None


def resolve_rows(
    rows: list[dict[str, Any]],
    *,
    with_location: bool = True,
    on_progress: OnProgress | None = None,
    deadline: float | None = None,
    should_stop: StopFn | None = None,
) -> TierResult:
    result = TierResult(
        tier="maps",
        inputs_passed=["company_name", "city"] if with_location else ["company_name"],
    )
    if not settings.rapidapi_key or not settings.maps_host:
        result.skipped = SKIP_NOT_CONFIGURED
        report_progress(on_progress, 0, len(rows), 0, {"requests_made": 0})
        return result
    total = len(rows)
    consecutive_timeouts = 0

    def _tick(idx: int) -> None:
        report_progress(
            on_progress,
            idx,
            total,
            len(result.candidates),
            {
                "rows_attempted": idx,
                "rows_done": result.rows_done,
                "requests_made": result.calls,
                "accepted": len(result.candidates),
            },
        )

    for idx, row in enumerate(rows, start=1):
        reason = _expired(deadline, should_stop)
        if reason:
            leftover = total - idx + 1
            result.none += leftover
            result.error = reason
            _tick(idx - 1)
            return result
        key = str(row.get("_source_key"))
        name = str(row.get("company_name") or "").strip()
        city = str(row.get("city") or "").strip() if with_location else ""
        if not name:
            result.none += 1
            result.rows_done += 1
            _tick(idx)
            continue
        query = f"{name} {city}".strip()
        try:
            hits = _search(query)
        except (VendorCallTimeout, VendorAcquireTimeout):
            result.calls += 1
            result.none += 1
            result.rows_done += 1
            consecutive_timeouts += 1
            if consecutive_timeouts >= CONSECUTIVE_TIMEOUT_LIMIT:
                leftover = total - idx
                result.none += leftover
                result.skipped = SKIP_UNRESPONSIVE
                result.error = "tier timeout"
                _tick(idx)
                return result
            _tick(idx)
            continue
        consecutive_timeouts = 0
        result.calls += 1
        result.billed_calls += 1
        if not hits:
            result.none += 1
            result.rows_done += 1
            _tick(idx)
            continue
        # Ranking is used; RapidAPI score is ignored. Gate decides at min_confidence=0.25.
        top = hits[0]
        place_id = str(top.get("place_id") or top.get("business_id") or "")
        domain = _pick_domain(top)
        detail: dict[str, Any] = {}
        if not domain and place_id:
            try:
                detail = _details(place_id)
            except (VendorCallTimeout, VendorAcquireTimeout):
                result.calls += 1
                result.none += 1
                result.rows_done += 1
                consecutive_timeouts += 1
                if consecutive_timeouts >= CONSECUTIVE_TIMEOUT_LIMIT:
                    leftover = total - idx
                    result.none += leftover
                    result.skipped = SKIP_UNRESPONSIVE
                    result.error = "tier timeout"
                    _tick(idx)
                    return result
                _tick(idx)
                continue
            result.calls += 1
            result.billed_calls += 1
            domain = _pick_domain({**top, **detail})
        result.rows_done += 1
        if not domain:
            result.none += 1
            _tick(idx)
            continue
        merged = {**top, **detail}
        result.candidates[key] = DomainCandidate(
            domain=domain,
            vendor_name=str(merged.get("name") or merged.get("title") or ""),
            title=str(merged.get("main_category") or merged.get("category") or ""),
            phone=_pick_phone(merged),
            address_state=str(merged.get("state") or merged.get("address_state") or ""),
            address_city=str(merged.get("city") or city),
            place_id=place_id,
            raw={"maps_score_ignored": True},
            inputs_passed=result.inputs_passed,
        )
        _tick(idx)
    return result
