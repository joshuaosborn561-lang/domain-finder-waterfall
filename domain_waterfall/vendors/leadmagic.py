"""LeadMagic company search. Prefer inline rows so the download URL header problem is avoided."""

from __future__ import annotations

import time
from typing import Any

from domain_waterfall import http_client
from domain_waterfall.config import settings
from domain_waterfall.normalize import extract_domain
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress

BASE = "https://api.leadmagic.io"


def _headers() -> dict[str, str]:
    return {
        "X-API-Key": settings.leadmagic_api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def live_unit_price() -> tuple[float, dict[str, Any]]:
    info: dict[str, Any] = {"configured": bool(settings.leadmagic_api_key)}
    if not settings.leadmagic_api_key:
        return 0.015, info
    r = http_client.get("leadmagic", f"{BASE}/v1/credits", headers=_headers(), timeout=20)
    if r is None:
        r = http_client.get("leadmagic", f"{BASE}/credits", headers=_headers(), timeout=20)
    per_credit = None
    if r is not None and r.status_code < 400:
        try:
            body = r.json()
        except ValueError:
            body = {}
        info["account"] = body if isinstance(body, dict) else None
        if isinstance(body, dict):
            info["credits"] = body.get("credits") or body.get("credit_balance")
            for key in ("credit_price", "price_per_credit", "usd_per_credit", "rate"):
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


def _parse_company(body: dict[str, Any]) -> tuple[str, str, str, str]:
    domain = extract_domain(
        str(
            body.get("companyDomain")
            or body.get("domain")
            or body.get("website")
            or body.get("company_domain")
            or ""
        )
    )
    name = str(body.get("companyName") or body.get("company_name") or body.get("name") or "")
    hq = body.get("headquarters") if isinstance(body.get("headquarters"), dict) else {}
    state = str(hq.get("state") or body.get("state") or "")
    phone = str(body.get("company_phone") or body.get("phone") or "")
    return domain, name, state, phone


def resolve_rows(
    rows: list[dict[str, Any]],
    *,
    unit: float = 0.015,
    on_progress: OnProgress | None = None,
) -> TierResult:
    result = TierResult(tier="leadmagic", inputs_passed=["company_name"])
    if not settings.leadmagic_api_key:
        result.skipped = "leadmagic_key_missing"
        return result
    payload_rows = [
        {"company_name": str(r.get("company_name") or ""), "identifier": str(r.get("_source_key"))}
        for r in rows
        if r.get("company_name")
    ]
    report_progress(on_progress, 0, len(rows), 0)
    if not payload_rows:
        report_progress(on_progress, len(rows), len(rows), 0)
        return result

    # Small/medium jobs: inline JSON. Avoids the fileUrl download-header trap.
    r = http_client.post(
        "leadmagic",
        f"{BASE}/bulk/submit",
        json={"product": "company_search", "rows": payload_rows},
        headers=_headers(),
        timeout=60,
    )
    result.calls += 1
    job_id = ""
    if r is not None and r.status_code < 400:
        try:
            body = r.json()
        except ValueError:
            body = {}
        job_id = str(
            (body or {}).get("jobId")
            or (body or {}).get("job_id")
            or (body or {}).get("id")
            or ""
        )
    if job_id:
        items = _poll_bulk(job_id, on_progress=on_progress, total=len(rows))
        found = 0
        for item in items:
            ident = str(item.get("identifier") or item.get("input_identifier") or "")
            domain, name, state, phone = _parse_company(item)
            if not domain:
                # nested
                inner = item.get("result") if isinstance(item.get("result"), dict) else item
                domain, name, state, phone = _parse_company(inner)
            if not ident or not domain:
                continue
            found += 1
            result.candidates[ident] = DomainCandidate(
                domain=domain,
                vendor_name=name,
                phone=phone,
                address_state=state,
                billed=True,
                cost_usd=unit,
                credits=1,
                inputs_passed=["company_name"],
            )
        result.billed_calls = found
        result.cost_usd = found * unit
        result.credits = float(found)
        result.none = len(payload_rows) - found
        report_progress(on_progress, len(rows), len(rows), len(result.candidates))
        return result

    # Fallback: single enrich_company (1 credit / match, free miss)
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        key = str(row.get("_source_key"))
        name = str(row.get("company_name") or "").strip()
        if not name:
            result.none += 1
            report_progress(on_progress, idx, total, len(result.candidates))
            continue
        sr = http_client.post(
            "leadmagic",
            f"{BASE}/v1/companies/company-search",
            json={"company_name": name},
            headers=_headers(),
            timeout=45,
        )
        result.calls += 1
        if sr is None or sr.status_code >= 400:
            result.none += 1
            report_progress(on_progress, idx, total, len(result.candidates))
            continue
        try:
            body = sr.json()
        except ValueError:
            result.none += 1
            report_progress(on_progress, idx, total, len(result.candidates))
            continue
        if not isinstance(body, dict):
            result.none += 1
            report_progress(on_progress, idx, total, len(result.candidates))
            continue
        msg = str(body.get("message") or "").lower()
        domain, vname, state, phone = _parse_company(body)
        if not domain or "not found" in msg:
            result.none += 1
            report_progress(on_progress, idx, total, len(result.candidates))
            continue
        result.candidates[key] = DomainCandidate(
            domain=domain,
            vendor_name=vname,
            phone=phone,
            address_state=state,
            billed=True,
            cost_usd=unit,
            credits=1,
            inputs_passed=["company_name"],
        )
        result.billed_calls += 1
        result.cost_usd += unit
        result.credits += 1
        report_progress(on_progress, idx, total, len(result.candidates))
    return result


def _poll_bulk(
    job_id: str,
    on_progress: OnProgress | None = None,
    total: int = 0,
) -> list[dict[str, Any]]:
    for _ in range(40):
        report_progress(on_progress, 0, total, 0)
        time.sleep(3)
        st = http_client.get(
            "leadmagic",
            f"{BASE}/bulk/jobs/{job_id}",
            headers=_headers(),
            timeout=30,
        )
        if st is None:
            continue
        try:
            body = st.json()
        except ValueError:
            continue
        status = str((body or {}).get("status") or (body or {}).get("state") or "").lower()
        if status in {"completed", "complete", "done", "succeeded", "success"}:
            rows = http_client.get(
                "leadmagic",
                f"{BASE}/bulk/jobs/{job_id}/rows",
                headers=_headers(),
                timeout=45,
            )
            if rows is None:
                return []
            try:
                payload = rows.json()
            except ValueError:
                return []
            if isinstance(payload, list):
                return [x for x in payload if isinstance(x, dict)]
            if isinstance(payload, dict):
                inner = payload.get("rows") or payload.get("data") or []
                return [x for x in inner if isinstance(x, dict)]
            return []
        if status in {"failed", "error"}:
            return []
    return []
