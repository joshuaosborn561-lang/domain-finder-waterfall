"""DiscoLike bulk-match-company-to-domain. Always pass location inputs when present."""

from __future__ import annotations

import csv
import io
import time
from typing import Any

from domain_waterfall import http_client
from domain_waterfall.config import settings
from domain_waterfall.normalize import extract_domain
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress

BASE = "https://api.discolike.com/v1"
UNIT = 0.00425


def _headers() -> dict[str, str]:
    return {"x-discolike-key": settings.discolike_api_key, "Accept": "application/json"}


def resolve_rows(
    rows: list[dict[str, Any]],
    *,
    with_location: bool = True,
    on_progress: OnProgress | None = None,
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
    payload = buf.getvalue().encode("utf-8")
    params = {
        "name_column": "name",
        "min_match_confidence": "80",
        "strict": "true",
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
        result.error = f"bulkmatch_http_{getattr(r, 'status_code', 0)}"
        result.none = len(rows)
        return result
    try:
        started = r.json()
    except ValueError:
        result.error = "bulkmatch_bad_json"
        result.none = len(rows)
        return result
    task_id = str(started.get("task_id") or "")
    if not task_id:
        result.error = "bulkmatch_no_task"
        result.none = len(rows)
        return result

    results: list[dict[str, Any]] = []
    for _ in range(60):
        report_progress(on_progress, 0, len(rows), 0)
        time.sleep(2)
        st = http_client.get(
            "discolike",
            f"{BASE}/bulkmatch/status/{task_id}",
            headers=_headers(),
            timeout=30,
        )
        if st is None:
            continue
        try:
            body = st.json()
        except ValueError:
            continue
        if not isinstance(body, dict):
            continue
        status = str(body.get("status") or "")
        if status == "completed":
            raw = body.get("results") or []
            results = [x for x in raw if isinstance(x, dict)]
            break
        if status == "failed":
            result.error = "bulkmatch_failed"
            break
    billed = 0
    seen: set[str] = set()
    for item in results:
        key = str(item.get("input:row_key") or item.get("row_key") or "")
        domain = extract_domain(str(item.get("domain") or item.get("website") or ""))
        if item.get("match_error"):
            continue
        billed += 1
        if not key or not domain:
            continue
        seen.add(key)
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
            cost_usd=UNIT,
        )
    # Billed on every query, including misses.
    result.billed_calls = len(rows)
    result.cost_usd = UNIT * len(rows)
    result.none = len(rows) - len(result.candidates)
    report_progress(on_progress, len(rows), len(rows), len(result.candidates))
    return result
