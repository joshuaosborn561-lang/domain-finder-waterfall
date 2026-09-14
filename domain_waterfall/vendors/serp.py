"""Apify google search scraper. Cache the actor input schema; on 400 refetch once, then fail."""

from __future__ import annotations

import time
from typing import Any, Callable

from domain_waterfall import http_client
from domain_waterfall.config import settings
from domain_waterfall.normalize import extract_domain
from domain_waterfall.concurrency import VendorThrottle, VendorTransportError
from domain_waterfall.tier_pool import (
    RowWorkResult,
    chunked,
    resolve_tier_concurrency,
    run_row_pool,
)
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress

_SCHEMA: dict[str, Any] | None = None
_SCHEMA_FAILED = False

StopFn = Callable[[], bool]
SERP_CHUNK = 100


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


def _start_run(queries: list[dict[str, str]]) -> str:
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
    if r is not None and r.status_code == 429:
        raise VendorThrottle("serp", "http 429")
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
            raise VendorTransportError("serp", "start failed after schema refresh")
    if r is not None and r.status_code == 429:
        raise VendorThrottle("serp", "http 429")
    if r is None or r.status_code >= 500:
        raise VendorTransportError("serp", f"start http {getattr(r, 'status_code', 'none')}")
    if r.status_code >= 400:
        raise VendorTransportError("serp", f"start http {r.status_code}")
    try:
        data = r.json()
    except ValueError as exc:
        raise VendorTransportError("serp", "bad json") from exc
    run = data.get("data") if isinstance(data, dict) else None
    run_id = str((run or {}).get("id") or "")
    if not run_id:
        raise VendorTransportError("serp", "missing run id")
    return run_id


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
        if r.status_code == 429:
            raise VendorThrottle("serp", "poll 429")
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
        raise VendorTransportError("serp", "poll timeout")
    r = http_client.get(
        "serp",
        f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true",
        headers=_headers(),
        timeout=45,
    )
    if r is None or r.status_code >= 500:
        raise VendorTransportError("serp", "dataset fetch failed")
    if r.status_code == 429:
        raise VendorThrottle("serp", "dataset 429")
    if r.status_code >= 400:
        return []
    try:
        items = r.json()
    except ValueError as exc:
        raise VendorTransportError("serp", "dataset json") from exc
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


def _resolve_chunk(
    chunk_row: dict[str, Any],
    *,
    unit: float,
    inputs: list[str],
) -> RowWorkResult:
    """One Apify chunk. chunk_row holds queries under _queries and a synthetic key."""
    queries: list[dict[str, str]] = list(chunk_row.get("_queries") or [])
    key = str(chunk_row.get("_source_key"))
    if not queries:
        return RowWorkResult(key=key, none=True, requests=0)
    run_id = _start_run(queries)
    items = _poll(run_id)
    by_query: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        search = str(item.get("searchQuery") or item.get("query") or "")
        by_query[search] = item
    # Pack per query outcomes into raw for the aggregator.
    packed: list[dict[str, Any]] = []
    none_n = 0
    hits_n = 0
    for q in queries:
        item = by_query.get(q["q"])
        domain, title = _domain_from_organic(item) if isinstance(item, dict) else ("", "")
        if not domain:
            none_n += 1
            packed.append({"key": q["key"], "none": True})
            continue
        hits_n += 1
        packed.append(
            {
                "key": q["key"],
                "domain": domain,
                "title": title,
                "cost_usd": unit,
            }
        )
    # Represent the chunk as a synthetic candidate carrier via raw.
    carrier = DomainCandidate(
        domain="",
        inputs_passed=inputs,
        billed=True,
        cost_usd=unit * len(queries),
        credits=0.0,
        raw={"serp_chunk": packed, "none": none_n, "hits": hits_n, "queries": len(queries)},
    )
    return RowWorkResult(
        key=key,
        candidate=carrier,
        none=False,
        requests=2 + len(queries),  # start + poll/dataset + billed queries
    )


def resolve_rows(
    rows: list[dict[str, Any]],
    *,
    with_location: bool = True,
    unit: float = 0.0045,
    on_progress: OnProgress | None = None,
    deadline: float | None = None,
    should_stop: StopFn | None = None,
    concurrency: int | None = None,
) -> TierResult:
    inputs = ["company_name"]
    if with_location:
        inputs.extend(["city", "state"])
    result = TierResult(
        tier="serp",
        inputs_passed=inputs,
        billing="apify google search, unit_usd from live_unit_price",
    )
    if not settings.apify_token:
        result.skipped = "apify_token_missing"
        return result
    cache_schema()
    # Build query snapshot from the fixed row list. Never re query source.
    queries: list[dict[str, str]] = []
    for row in rows:
        name = str(row.get("company_name") or "").strip()
        city = str(row.get("city") or "").strip() if with_location else ""
        state = str(row.get("state") or "").strip() if with_location else ""
        q = f'"{name}"'
        if city or state:
            q = f'"{name}" {city} {state}'.strip()
        queries.append({"key": str(row.get("_source_key")), "q": q, "name": name})

    chunks = chunked(queries, SERP_CHUNK)
    chunk_rows: list[dict[str, Any]] = [
        {"_source_key": f"serp_chunk_{i}", "_queries": ch} for i, ch in enumerate(chunks)
    ]
    workers = resolve_tier_concurrency("serp", concurrency)

    def _one(chunk_row: dict[str, Any]) -> RowWorkResult:
        return _resolve_chunk(chunk_row, unit=unit, inputs=inputs)

    report_progress(on_progress, 0, len(rows), 0, {"requests_made": 0, "errored": 0})
    pooled = run_row_pool(
        chunk_rows,
        _one,
        tier="serp",
        concurrency=workers,
        on_progress=None,  # remap progress onto row counts below
        should_stop=should_stop,
        deadline=deadline,
        result=result,
    )

    # Unpack chunk carriers into per row candidates.
    final = TierResult(
        tier="serp",
        inputs_passed=inputs,
        billing=result.billing,
        skipped=pooled.skipped,
        error=pooled.error,
        calls=pooled.calls,
        billed_calls=0,
        cost_usd=0.0,
        credits=0.0,
        errored=pooled.errored,
    )
    done_rows = 0
    for carrier in pooled.candidates.values():
        packed = (carrier.raw or {}).get("serp_chunk") or []
        for item in packed:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key"))
            if item.get("none"):
                final.none += 1
                done_rows += 1
                continue
            domain = str(item.get("domain") or "")
            if not domain:
                final.none += 1
                done_rows += 1
                continue
            cost = float(item.get("cost_usd") or unit)
            final.candidates[key] = DomainCandidate(
                domain=domain,
                vendor_name="",
                title=str(item.get("title") or ""),
                inputs_passed=inputs,
                billed=True,
                cost_usd=cost,
            )
            final.cost_usd += cost
            final.billed_calls += 1
            done_rows += 1
    # Chunks that errored after retries: count their queries as errored, not none.
    errored_chunks = pooled.errored
    if errored_chunks:
        # Approximate: each errored chunk covers up to SERP_CHUNK queries still unaccounted.
        remaining = len(rows) - done_rows
        take = min(remaining, errored_chunks * SERP_CHUNK)
        final.errored += take
        done_rows += take
    final.rows_done = done_rows
    report_progress(
        on_progress,
        min(done_rows, len(rows)),
        len(rows),
        len(final.candidates),
        {
            "rows_done": final.rows_done,
            "accepted": len(final.candidates),
            "requests_made": final.calls,
            "errored": final.errored,
            "none": final.none,
        },
    )
    return final
