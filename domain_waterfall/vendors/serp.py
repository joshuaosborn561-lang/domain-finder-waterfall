"""Apify google-search-scraper. Cache the actor input schema; on 400 refetch once, then fail."""

from __future__ import annotations

import time
from typing import Any

from domain_waterfall import http_client
from domain_waterfall.config import settings
from domain_waterfall.normalize import extract_domain
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress

_SCHEMA: dict[str, Any] | None = None
_SCHEMA_FAILED = False


def _actor_id() -> str:
    return settings.apify_actor.replace("/", "~")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.apify_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def cache_schema() -> dict[str, Any] | None:
    global _SCHEMA, _SCHEMA_FAILED
    if _SCHEMA is not None or _SCHEMA_FAILED:
        return _SCHEMA
    if not settings.apify_token:
        return None
    r = http_client.get(
        "serp",
        f"https://api.apify.com/v2/acts/{_actor_id()}",
        headers=_headers(),
        timeout=30,
    )
    if r is None or r.status_code >= 400:
        _SCHEMA_FAILED = True
        return None
    try:
        _SCHEMA = r.json()
    except ValueError:
        _SCHEMA_FAILED = True
        return None
    return _SCHEMA


def live_unit_price() -> tuple[float, dict[str, Any]]:
    info: dict[str, Any] = {"configured": bool(settings.apify_token)}
    schema = cache_schema()
    unit = 0.0045
    if isinstance(schema, dict):
        data = schema.get("data") if isinstance(schema.get("data"), dict) else schema
        pricing = data.get("pricingInfos") or data.get("exampleRunInput") or {}
        info["pricing"] = pricing if isinstance(pricing, (dict, list)) else None
    info["unit_usd"] = unit
    info["source"] = "published_fallback" if unit == 0.0045 else "live"
    return unit, info


def _start_run(queries: list[dict[str, str]]) -> tuple[str | None, bool]:
    body = {
        "queries": "\n".join(q["q"] for q in queries),
        "maxPagesPerQuery": 1,
        "resultsPerPage": 10,
        "mobileResults": False,
        "languageCode": "en",
        "countryCode": "us",
    }
    r = http_client.post(
        "serp",
        f"https://api.apify.com/v2/acts/{_actor_id()}/runs?waitForFinish=0",
        json=body,
        headers=_headers(),
        timeout=30,
    )
    if r is not None and r.status_code == 400:
        cache_schema()
        r = http_client.post(
            "serp",
            f"https://api.apify.com/v2/acts/{_actor_id()}/runs?waitForFinish=0",
            json=body,
            headers=_headers(),
            timeout=30,
        )
        if r is None or r.status_code == 400:
            return None, True
    if r is None or r.status_code >= 400:
        return None, True
    try:
        data = r.json()
    except ValueError:
        return None, True
    run = data.get("data") if isinstance(data, dict) else None
    run_id = str((run or {}).get("id") or "")
    return (run_id or None), False


def _poll(run_id: str) -> list[dict[str, Any]]:
    dataset_id = ""
    for _ in range(40):
        time.sleep(3)
        r = http_client.get(
            "serp",
            f"https://api.apify.com/v2/actor-runs/{run_id}",
            headers=_headers(),
            timeout=20,
        )
        if r is None:
            continue
        try:
            data = r.json()
        except ValueError:
            continue
        body = data.get("data") if isinstance(data, dict) else {}
        status = str((body or {}).get("status") or "")
        if status in {"SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"}:
            dataset_id = str(((body or {}).get("defaultDatasetId") or ""))
            break
    if not dataset_id:
        return []
    r = http_client.get(
        "serp",
        f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true",
        headers=_headers(),
        timeout=45,
    )
    if r is None or r.status_code >= 400:
        return []
    try:
        items = r.json()
    except ValueError:
        return []
    return items if isinstance(items, list) else []


def _domain_from_organic(item: dict[str, Any]) -> tuple[str, str]:
    organic = item.get("organicResults") or item.get("organic") or []
    if not isinstance(organic, list):
        return "", ""
    for hit in organic:
        if not isinstance(hit, dict):
            continue
        url = str(hit.get("url") or hit.get("link") or "")
        domain = extract_domain(url)
        title = str(hit.get("title") or "")
        if domain:
            return domain, title
    return "", ""


def resolve_rows(
    rows: list[dict[str, Any]],
    *,
    with_location: bool = True,
    unit: float = 0.0045,
    on_progress: OnProgress | None = None,
) -> TierResult:
    inputs = ["company_name"]
    if with_location:
        inputs.extend(["city", "state"])
    result = TierResult(tier="serp", inputs_passed=inputs)
    if not settings.apify_token:
        result.skipped = "apify_token_missing"
        return result
    cache_schema()
    queries: list[dict[str, str]] = []
    for row in rows:
        name = str(row.get("company_name") or "").strip()
        city = str(row.get("city") or "").strip() if with_location else ""
        state = str(row.get("state") or "").strip() if with_location else ""
        q = f'"{name}"'
        if city or state:
            q = f'"{name}" {city} {state}'.strip()
        queries.append({"key": str(row.get("_source_key")), "q": q, "name": name})

    report_progress(on_progress, 0, len(rows), 0)
    # Batch 90–110 per run
    for i in range(0, len(queries), 100):
        chunk = queries[i : i + 100]
        run_id, failed = _start_run(chunk)
        result.calls += 1
        if failed or not run_id:
            result.error = "serp_start_failed"
            result.none += len(chunk)
            continue
        items = _poll(run_id)
        result.billed_calls += len(chunk)
        result.cost_usd += unit * len(chunk)
        by_query: dict[str, dict[str, Any]] = {}
        for item in items:
            if not isinstance(item, dict):
                continue
            search = str(item.get("searchQuery") or item.get("query") or "")
            by_query[search] = item
        for q in chunk:
            item = by_query.get(q["q"])
            if item is None:
                # fuzzy: first unused
                item = next(iter(items), None) if items else None
            domain, title = _domain_from_organic(item) if isinstance(item, dict) else ("", "")
            if not domain:
                result.none += 1
                continue
            result.candidates[q["key"]] = DomainCandidate(
                domain=domain,
                vendor_name="",
                title=title,
                inputs_passed=inputs,
                billed=True,
                cost_usd=unit,
            )
        report_progress(on_progress, min(i + len(chunk), len(rows)), len(rows), len(result.candidates))
    return result
