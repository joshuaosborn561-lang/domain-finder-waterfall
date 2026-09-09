"""Prospeo bulk-enrich-company. Free on miss. 25 per call as specified."""

from __future__ import annotations

from typing import Any

from domain_waterfall import http_client
from domain_waterfall.config import settings
from domain_waterfall.normalize import extract_domain
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress

BASE = "https://api.prospeo.io"
BATCH = 25


def _headers() -> dict[str, str]:
    return {
        "X-KEY": settings.prospeo_api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def live_unit_price() -> tuple[float, dict[str, Any]]:
    info: dict[str, Any] = {"configured": bool(settings.prospeo_api_key)}
    if not settings.prospeo_api_key:
        return 0.015, info
    r = http_client.get("prospeo", f"{BASE}/account-information", headers=_headers(), timeout=20)
    per_credit = None
    if r is not None and r.status_code < 400:
        try:
            body = r.json()
        except ValueError:
            body = {}
        info["account"] = {k: body.get(k) for k in ("credits", "plan", "credit_price") if isinstance(body, dict)}
        if isinstance(body, dict):
            for key in ("credit_price", "price_per_credit", "usd_per_credit"):
                if body.get(key) not in (None, ""):
                    try:
                        per_credit = float(body[key])
                    except (TypeError, ValueError):
                        pass
    if per_credit is None:
        per_credit = 0.015
        info["rate_source"] = "mid_band"
    else:
        info["rate_source"] = "account"
    info["per_credit_usd"] = per_credit
    return per_credit, info


def resolve_rows(
    rows: list[dict[str, Any]],
    *,
    guessed: dict[str, str] | None = None,
    unit: float = 0.015,
    on_progress: OnProgress | None = None,
) -> TierResult:
    result = TierResult(tier="prospeo", inputs_passed=["company_name"])
    if not settings.prospeo_api_key:
        result.skipped = "prospeo_key_missing"
        return result
    guessed = guessed or {}
    report_progress(on_progress, 0, len(rows), 0)
    for i in range(0, len(rows), BATCH):
        chunk = rows[i : i + BATCH]
        payload = []
        for row in chunk:
            item: dict[str, str] = {
                "identifier": str(row.get("_source_key")),
                "company_name": str(row.get("company_name") or ""),
            }
            prior = guessed.get(str(row.get("_source_key")))
            if prior:
                item["company_website"] = prior
                if "company_website" not in result.inputs_passed:
                    result.inputs_passed.append("company_website")
            payload.append(item)
        r = http_client.post(
            "prospeo",
            f"{BASE}/bulk-enrich-company",
            json={"data": payload},
            headers=_headers(),
            timeout=60,
        )
        result.calls += 1
        if r is None or r.status_code >= 400:
            result.none += len(chunk)
            continue
        try:
            body = r.json()
        except ValueError:
            result.none += len(chunk)
            continue
        matched = body.get("matched") if isinstance(body, dict) else None
        found_keys: set[str] = set()
        if isinstance(matched, list):
            for item in matched:
                if not isinstance(item, dict):
                    continue
                ident = str(item.get("identifier") or "")
                company = item.get("company") if isinstance(item.get("company"), dict) else {}
                domain = extract_domain(
                    str(company.get("domain") or company.get("website") or "")
                )
                if not ident or not domain:
                    continue
                found_keys.add(ident)
                loc = company.get("location") if isinstance(company.get("location"), dict) else {}
                result.candidates[ident] = DomainCandidate(
                    domain=domain,
                    vendor_name=str(company.get("name") or ""),
                    phone=str(company.get("phone") or company.get("phone_number") or ""),
                    address_state=str(loc.get("state") or ""),
                    address_city=str(loc.get("city") or ""),
                    billed=True,
                    cost_usd=unit,
                    credits=1,
                    inputs_passed=result.inputs_passed,
                )
        cost = body.get("total_cost") if isinstance(body, dict) else None
        try:
            credits = float(cost) if cost is not None else float(len(found_keys))
        except (TypeError, ValueError):
            credits = float(len(found_keys))
        result.credits += credits
        result.cost_usd += credits * unit
        result.billed_calls += int(credits)
        result.none += len(chunk) - len(found_keys)
        report_progress(on_progress, min(i + len(chunk), len(rows)), len(rows), len(result.candidates))
    return result
