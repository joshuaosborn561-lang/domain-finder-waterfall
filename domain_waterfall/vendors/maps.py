"""Maps resolve_places via RapidAPI Maps Data. Score is ignored; ranking is used.

Maps is free on this plan (RapidAPI quota, unit_usd 0). cost_usd and credits stay 0.
"""

from __future__ import annotations

import time
from typing import Any, Callable
from urllib.parse import urlencode

from domain_waterfall import http_client
from domain_waterfall.concurrency import (
    VendorAcquireTimeout,
    VendorCallTimeout,
    VendorThrottle,
    VendorTransportError,
)
from domain_waterfall.config import settings
from domain_waterfall.normalize import extract_domain
from domain_waterfall.tier_pool import (
    RowWorkResult,
    resolve_tier_concurrency,
    run_row_pool,
)
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress

MAPS_CALL_TIMEOUT_S = 15
MAPS_MAX_ATTEMPTS = 1
SKIP_NOT_CONFIGURED = "not configured"

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


def _parse_search_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    # Soft throttle bodies often look like {"message": "..."} with no results.
    for bad in ("message", "error", "errors"):
        if bad in payload and not any(k in payload for k in ("data", "results", "items")):
            raise VendorThrottle("maps", f"soft throttle body key {bad}")
    for key in ("data", "results", "items"):
        val = payload.get(key)
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]
        if isinstance(val, dict) and isinstance(val.get("items"), list):
            return [x for x in val["items"] if isinstance(x, dict)]
    return []


def _search(query: str, *, empty_retries: int = 1) -> list[dict[str, Any]]:
    params = {
        "query": query,
        "limit": "8",
        "country": "us",
        "lang": "en",
        "offset": "0",
        "zoom": "13",
    }
    url = f"https://{settings.maps_host}/searchmaps.php?{urlencode(params)}"
    last_empty = False
    for attempt in range(max(1, empty_retries)):
        t0 = time.monotonic()
        try:
            r = _maps_get(url)
        except VendorThrottle:
            raise
        except VendorCallTimeout as exc:
            raise VendorTransportError("maps", str(exc)) from exc
        except VendorAcquireTimeout as exc:
            raise VendorTransportError("maps", str(exc)) from exc
        elapsed = time.monotonic() - t0
        if r is None:
            raise VendorTransportError("maps", "empty response")
        if r.status_code == 429:
            raise VendorThrottle("maps", "http 429")
        if r.status_code >= 500:
            raise VendorTransportError("maps", f"http {r.status_code}")
        if r.status_code >= 400:
            # Client errors are true misses for this query, not transport failures.
            return []
        try:
            payload = r.json()
        except ValueError as exc:
            raise VendorTransportError("maps", "bad json") from exc
        try:
            hits = _parse_search_payload(payload)
        except VendorThrottle:
            raise
        if hits:
            return hits
        last_empty = True
        # Retry empty results once; under load RapidAPI sometimes returns a blank page.
        if attempt < empty_retries - 1:
            time.sleep(0.5 * (attempt + 1) + (0.15 if elapsed < 0.5 else 0.0))
            continue
    return [] if last_empty else []


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
    try:
        r = _maps_get(url)
    except VendorThrottle:
        raise
    except VendorCallTimeout as exc:
        raise VendorTransportError("maps", str(exc)) from exc
    except VendorAcquireTimeout as exc:
        raise VendorTransportError("maps", str(exc)) from exc
    if r is None:
        raise VendorTransportError("maps", "empty response")
    if r.status_code == 429:
        raise VendorThrottle("maps", "http 429")
    if r.status_code >= 500:
        raise VendorTransportError("maps", f"http {r.status_code}")
    if r.status_code >= 400:
        return {}
    try:
        payload = r.json()
    except ValueError as exc:
        raise VendorTransportError("maps", "bad json") from exc
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


def resolve_one_row(row: dict[str, Any], *, with_location: bool, inputs: list[str]) -> RowWorkResult:
    """Single row resolution. Matching and gate stay unchanged upstream."""
    key = str(row.get("_source_key"))
    name = str(row.get("company_name") or "").strip()
    city = str(row.get("city") or "").strip() if with_location else ""
    if not name:
        return RowWorkResult(key=key, none=True, requests=0)
    query = f"{name} {city}".strip()
    requests = 0
    hits = _search(query)
    requests += 1
    if not hits:
        return RowWorkResult(key=key, none=True, requests=requests)
    # Ranking is used; RapidAPI score is ignored. Gate decides at min_confidence=0.25.
    top = hits[0]
    place_id = str(top.get("place_id") or top.get("business_id") or "")
    domain = _pick_domain(top)
    detail: dict[str, Any] = {}
    if not domain and place_id:
        detail = _details(place_id)
        requests += 1
        domain = _pick_domain({**top, **detail})
    if not domain:
        return RowWorkResult(key=key, none=True, requests=requests)
    merged = {**top, **detail}
    cand = DomainCandidate(
        domain=domain,
        vendor_name=str(merged.get("name") or merged.get("title") or ""),
        title=str(merged.get("main_category") or merged.get("category") or ""),
        phone=_pick_phone(merged),
        address_state=str(merged.get("state") or merged.get("address_state") or ""),
        address_city=str(merged.get("city") or city),
        place_id=place_id,
        raw={"maps_score_ignored": True},
        inputs_passed=inputs,
        billed=False,
        cost_usd=0.0,
        credits=0.0,
    )
    return RowWorkResult(key=key, candidate=cand, requests=requests)


def resolve_rows(
    rows: list[dict[str, Any]],
    *,
    with_location: bool = True,
    on_progress: OnProgress | None = None,
    deadline: float | None = None,
    should_stop: StopFn | None = None,
    concurrency: int | None = None,
) -> TierResult:
    inputs = ["company_name", "city"] if with_location else ["company_name"]
    result = TierResult(
        tier="maps",
        inputs_passed=inputs,
        cost_usd=0.0,
        credits=0.0,
        billing="free, RapidAPI maps quota, unit_usd 0",
    )
    if not settings.rapidapi_key or not settings.maps_host:
        result.skipped = SKIP_NOT_CONFIGURED
        report_progress(on_progress, 0, len(rows), 0, {"requests_made": 0, "errored": 0})
        return result

    workers = resolve_tier_concurrency("maps", concurrency)

    def _one(row: dict[str, Any]) -> RowWorkResult:
        return resolve_one_row(row, with_location=with_location, inputs=inputs)

    # Snapshot only: rows is the fixed list from job start. Never re query source.
    return run_row_pool(
        list(rows),
        _one,
        tier="maps",
        concurrency=workers,
        on_progress=on_progress,
        should_stop=should_stop,
        deadline=deadline,
        result=result,
    )
