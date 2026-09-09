"""Maps resolve_places via RapidAPI Maps Data. Score is ignored; ranking is used."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlencode

from domain_waterfall import http_client
from domain_waterfall.config import settings
from domain_waterfall.normalize import extract_domain
from domain_waterfall.vendors.base import DomainCandidate, TierResult


def _headers() -> dict[str, str]:
    return {
        "x-rapidapi-key": settings.rapidapi_key,
        "x-rapidapi-host": settings.maps_host,
        "Accept": "application/json",
    }


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
    r = http_client.get("maps", url, headers=_headers(), timeout=30)
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
    r = http_client.get("maps", url, headers=_headers(), timeout=30)
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


def _pick_domain(row: dict[str, Any]) -> str:
    for key in ("website", "domain", "site", "url"):
        val = row.get(key)
        if isinstance(val, str) and val.strip():
            return extract_domain(val)
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


def resolve_rows(rows: list[dict[str, Any]], *, with_location: bool = True) -> TierResult:
    result = TierResult(
        tier="maps",
        inputs_passed=["company_name", "city"] if with_location else ["company_name"],
    )
    if not settings.rapidapi_key:
        result.skipped = "maps_key_missing"
        return result
    for row in rows:
        key = str(row.get("_source_key"))
        name = str(row.get("company_name") or "").strip()
        city = str(row.get("city") or "").strip() if with_location else ""
        if not name:
            result.none += 1
            continue
        query = f"{name} {city}".strip()
        hits = _search(query)
        result.calls += 1
        result.billed_calls += 1
        if not hits:
            result.none += 1
            continue
        top = hits[0]
        place_id = str(top.get("place_id") or top.get("business_id") or "")
        detail = _details(place_id) if place_id else {}
        result.calls += 1
        result.billed_calls += 1
        merged = {**top, **detail}
        domain = _pick_domain(merged)
        if not domain:
            result.none += 1
            continue
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
    return result
