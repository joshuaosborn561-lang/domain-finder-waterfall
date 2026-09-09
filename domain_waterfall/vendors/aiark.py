"""AI Ark Company Search. Do not guess enum values; location uses documented city/state/country strings."""

from __future__ import annotations

from typing import Any

from domain_waterfall import http_client
from domain_waterfall.config import settings
from domain_waterfall.normalize import e164_us, extract_domain
from domain_waterfall.profiles import ClientProfile
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress

BASE = "https://api.ai-ark.com/api/developer-portal"


def _headers() -> dict[str, str]:
    return {
        "X-TOKEN": settings.ai_ark_api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def live_unit_price() -> tuple[float, dict[str, Any]]:
    """Read credits + infer per-credit rate from the account. Company search is 0.1 credit."""
    if not settings.ai_ark_api_key:
        return 0.0005, {"configured": False}
    r = http_client.get("aiark", f"{BASE}/v1/payments/credits", headers=_headers(), timeout=20)
    info: dict[str, Any] = {"configured": True}
    if r is None:
        return 0.0005, {**info, "live": False}
    try:
        body = r.json()
    except ValueError:
        body = {}
    info["credits"] = body
    # Published band $0.002–$0.0098 per credit. Prefer a reported rate if present.
    per_credit = None
    if isinstance(body, dict):
        for key in ("creditPrice", "credit_price", "price_per_credit", "usdPerCredit"):
            if body.get(key) not in (None, ""):
                try:
                    per_credit = float(body[key])
                except (TypeError, ValueError):
                    pass
    if per_credit is None:
        per_credit = 0.005  # mid-band until the account reports a rate
        info["rate_source"] = "mid_band"
    else:
        info["rate_source"] = "account"
    info["per_credit_usd"] = per_credit
    return round(0.1 * per_credit, 6), info


def _search(body: dict[str, Any]) -> list[dict[str, Any]]:
    r = http_client.post("aiark", f"{BASE}/v1/companies", json=body, headers=_headers(), timeout=45)
    if r is None or r.status_code >= 400:
        return []
    try:
        data = r.json()
    except ValueError:
        return []
    content = data.get("content") if isinstance(data, dict) else None
    if isinstance(content, list):
        return [x for x in content if isinstance(x, dict)]
    return []


def resolve_rows(
    rows: list[dict[str, Any]],
    profile: ClientProfile,
    *,
    with_location: bool = True,
    unit: float = 0.0005,
    on_progress: OnProgress | None = None,
) -> TierResult:
    inputs = ["company_name"]
    if with_location:
        inputs.append("companyLocation")
        if profile.center_lat is not None:
            inputs.append("geo_circle")
    result = TierResult(tier="aiark", inputs_passed=inputs)
    if not settings.ai_ark_api_key:
        result.skipped = "aiark_key_missing"
        report_progress(on_progress, 0, len(rows), 0)
        return result
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        key = str(row.get("_source_key"))
        name = str(row.get("company_name") or "").strip()
        if not name:
            result.none += 1
            report_progress(on_progress, idx, total, len(result.candidates))
            continue
        account: dict[str, Any] = {
            "name": {"any": {"include": {"mode": "SMART", "content": [name]}}},
        }
        phone = e164_us(str(row.get("phone") or ""))
        if phone.startswith("+"):
            account["phoneNumber"] = {"any": {"include": [phone]}}
            if "companyPhoneNumber" not in result.inputs_passed:
                result.inputs_passed.append("companyPhoneNumber")
        if with_location:
            locations: list[str] = []
            city = str(row.get("city") or "").strip()
            state = str(row.get("state") or "").strip()
            if city:
                locations.append(city)
            if state:
                locations.append(state)
            country = str(row.get("country") or profile.country or "").strip()
            if country:
                locations.append(country)
            if locations:
                # Documented vocabulary: city / state / country strings. Not guessed enums.
                account["location"] = {"any": {"include": locations[:8]}}
            if (
                profile.center_lat is not None
                and profile.center_lng is not None
                and profile.radius_mi
            ):
                account["geoLocation"] = {
                    "position": {"lat": profile.center_lat, "lng": profile.center_lng},
                    "radius": profile.radius_mi,
                    "unit": "mi",
                }
        hits = _search({"account": account, "page": 0, "size": 1})
        result.calls += 1
        result.credits += 0.1 if hits else 0.0
        result.cost_usd += unit if hits else 0.0
        result.billed_calls += 1 if hits else 0
        if not hits:
            result.none += 1
            report_progress(on_progress, idx, total, len(result.candidates))
            continue
        top = hits[0]
        summary = top.get("summary") if isinstance(top.get("summary"), dict) else {}
        link = top.get("link") if isinstance(top.get("link"), dict) else {}
        loc = top.get("location") if isinstance(top.get("location"), dict) else {}
        hq = loc.get("headquarter") if isinstance(loc.get("headquarter"), dict) else {}
        domain = extract_domain(str(link.get("domain") or link.get("website") or ""))
        if not domain:
            result.none += 1
            report_progress(on_progress, idx, total, len(result.candidates))
            continue
        result.candidates[key] = DomainCandidate(
            domain=domain,
            vendor_name=str(summary.get("name") or ""),
            title=str(summary.get("industry") or ""),
            phone=phone,
            address_state=str(hq.get("state") or ""),
            address_city=str(hq.get("city") or ""),
            raw={"id": top.get("id")},
            inputs_passed=result.inputs_passed,
            billed=bool(hits),
            cost_usd=unit if hits else 0.0,
            credits=0.1 if hits else 0.0,
        )
        report_progress(on_progress, idx, total, len(result.candidates))
    return result
