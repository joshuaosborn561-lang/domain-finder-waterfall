"""Apify google search scraper. One async actor run per query batch, then poll."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Callable

from domain_waterfall import http_client
from domain_waterfall.config import settings
from domain_waterfall.gate import GLOBAL_BLOCKLIST, is_blocklisted
from domain_waterfall.normalize import distinctive_tokens, domain_name_part, extract_domain
from domain_waterfall.concurrency import (
    VendorCallTimeout,
    VendorThrottle,
    VendorTransportError,
)
from domain_waterfall.tier_pool import (
    RowWorkResult,
    chunked,
    resolve_tier_concurrency,
    run_row_pool,
)
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress

log = logging.getLogger("domain_waterfall.serp")

_SCHEMA: dict[str, Any] | None = None
_SCHEMA_FAILED = False

StopFn = Callable[[], bool]
SERP_CHUNK = 100
# A single google-search-scraper query is 45s plus. 120s was too short for a batch.
SERP_POLL_INTERVAL_S = 5.0
SERP_POLL_MIN_S = 900.0
SERP_POLL_PER_QUERY_S = 50.0
SERP_POLL_CAP_S = 45 * 60
SERP_START_TIMEOUT_S = 30
SERP_DEFAULT_RUN_CONCURRENCY = 2
RUNS_URL = "https://api.apify.com/v2/acts/{actor}/runs"


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


def _runs_url() -> str:
    # No waitForFinish: return runId immediately. A sync wait dies on a 30s HTTP timeout.
    return RUNS_URL.format(actor=_actor_id())


def _run_url(run_id: str) -> str:
    return f"https://api.apify.com/v2/actor-runs/{run_id}"


def poll_budget_s(n_queries: int) -> float:
    n = max(1, int(n_queries))
    return min(SERP_POLL_CAP_S, max(SERP_POLL_MIN_S, n * SERP_POLL_PER_QUERY_S))


def _start_run(queries: list[dict[str, str]]) -> str:
    url = _runs_url()
    body = {
        "queries": "\n".join(q["q"] for q in queries),
        "maxPagesPerQuery": 1,
        "resultsPerPage": 10,
        "mobileResults": False,
        "languageCode": "en",
        "countryCode": "us",
    }
    try:
        r = http_client.post(
            "serp",
            url,
            json=body,
            headers=_headers(),
            timeout=SERP_START_TIMEOUT_S,
        )
    except VendorCallTimeout as exc:
        raise VendorTransportError(
            "serp",
            status="timeout",
            message=str(exc),
            timeout=float(SERP_START_TIMEOUT_S),
            url=url,
        ) from exc
    if r is not None and r.status_code == 429:
        raise VendorThrottle("serp", "http 429")
    if r is not None and r.status_code == 400:
        cache_schema()
        r = http_client.post(
            "serp",
            url,
            json=body,
            headers=_headers(),
            timeout=SERP_START_TIMEOUT_S,
        )
        if r is None or r.status_code == 400:
            body_text = ""
            if r is not None:
                try:
                    body_text = (r.text or "")[:300]
                except Exception:
                    body_text = ""
            raise VendorTransportError(
                "serp",
                status=400,
                message=body_text or "start failed after schema refresh",
                url=url,
            )
    if r is not None and r.status_code == 429:
        raise VendorThrottle("serp", "http 429")
    if r is None:
        raise VendorTransportError(
            "serp",
            status="none",
            message="start returned no response",
            timeout=float(SERP_START_TIMEOUT_S),
            url=url,
        )
    if r.status_code >= 400:
        snippet = ""
        try:
            snippet = (r.text or "")[:300]
        except Exception:
            snippet = ""
        raise VendorTransportError(
            "serp",
            status=r.status_code,
            message=snippet or f"start http {r.status_code}",
            url=url,
        )
    try:
        data = r.json()
    except ValueError as exc:
        raise VendorTransportError("serp", message="bad json", url=url) from exc
    run = data.get("data") if isinstance(data, dict) else None
    run_id = str((run or {}).get("id") or "")
    if not run_id:
        raise VendorTransportError("serp", message="missing run id", url=url)
    log.info("serp started run_id=%s queries=%s", run_id, len(queries))
    return run_id


def _poll(
    run_id: str,
    *,
    n_queries: int,
    deadline: float | None = None,
) -> list[dict[str, Any]]:
    url = _run_url(run_id)
    budget = poll_budget_s(n_queries)
    started = time.monotonic()
    last_status = ""
    dataset_id = ""
    while time.monotonic() - started < budget:
        if deadline is not None and time.monotonic() >= deadline:
            raise VendorTransportError(
                "serp",
                status=last_status or "deadline",
                message="job deadline during poll",
                timeout=round(time.monotonic() - started, 1),
                url=url,
            )
        try:
            r = http_client.get("serp", url, headers=_headers(), timeout=20)
        except VendorCallTimeout as exc:
            log.warning("serp poll http timeout run_id=%s, %s", run_id, exc)
            time.sleep(SERP_POLL_INTERVAL_S)
            continue
        except VendorTransportError:
            raise
        if r is None:
            time.sleep(SERP_POLL_INTERVAL_S)
            continue
        if r.status_code == 429:
            raise VendorThrottle("serp", "poll 429")
        try:
            data = r.json()
        except ValueError:
            time.sleep(SERP_POLL_INTERVAL_S)
            continue
        body = data.get("data") if isinstance(data, dict) else {}
        body = body if isinstance(body, dict) else {}
        last_status = str(body.get("status") or "")
        dataset_id = str(body.get("defaultDatasetId") or dataset_id)
        if last_status == "SUCCEEDED":
            break
        if last_status in {"FAILED", "ABORTED", "TIMED-OUT"}:
            raise VendorTransportError(
                "serp",
                status=last_status,
                message=str(body.get("statusMessage") or last_status),
                url=url,
            )
        time.sleep(SERP_POLL_INTERVAL_S)
    else:
        raise VendorTransportError(
            "serp",
            status=last_status or "RUNNING",
            message="poll timeout",
            timeout=budget,
            url=url,
        )
    if not dataset_id:
        raise VendorTransportError(
            "serp",
            status=last_status or "SUCCEEDED",
            message="missing dataset id",
            url=url,
        )
    items_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?clean=true"
    r = http_client.get("serp", items_url, headers=_headers(), timeout=45)
    if r is None:
        raise VendorTransportError(
            "serp",
            status="none",
            message="dataset fetch failed",
            timeout=45,
            url=items_url,
        )
    if r.status_code == 429:
        raise VendorThrottle("serp", "dataset 429")
    if r.status_code >= 400:
        raise VendorTransportError(
            "serp",
            status=r.status_code,
            message="dataset fetch failed",
            url=items_url,
        )
    try:
        items = r.json()
    except ValueError as exc:
        raise VendorTransportError("serp", message="dataset json", url=items_url) from exc
    log.info(
        "serp run_id=%s status=%s items=%s",
        run_id,
        last_status or "SUCCEEDED",
        len(items) if isinstance(items, list) else 0,
    )
    return items if isinstance(items, list) else []


def _query_term(item: dict[str, Any]) -> str:
    raw = item.get("searchQuery")
    if raw is None:
        raw = item.get("query")
    if isinstance(raw, dict):
        return str(raw.get("term") or raw.get("query") or "").strip()
    return str(raw or "").strip()


def _norm_q(text: str) -> str:
    return " ".join(str(text or "").replace('"', " ").split()).lower()


def _item_for_query(by_query: dict[str, dict[str, Any]], q: str) -> dict[str, Any] | None:
    if q in by_query:
        return by_query[q]
    want = _norm_q(q)
    for key, item in by_query.items():
        if _norm_q(key) == want:
            return item
    return None


def build_serp_query(name: str, city: str = "", state: str = "") -> str:
    """Always company + city + state. Never the company name alone."""
    return " ".join(p for p in ((name or "").strip(), (city or "").strip(), (state or "").strip()) if p)


def _domain_from_organic(item: dict[str, Any], company_name: str = "") -> tuple[str, str]:
    organic = item.get("organicResults") or item.get("organic") or []
    if not isinstance(organic, list):
        return "", ""
    toks = distinctive_tokens(company_name, [])
    for hit in organic:
        if not isinstance(hit, dict):
            continue
        url = str(hit.get("url") or hit.get("link") or "")
        domain = extract_domain(url)
        if not domain or is_blocklisted(domain, GLOBAL_BLOCKLIST):
            continue
        title = str(hit.get("title") or "")
        part = domain_name_part(domain)
        if toks and not any(tok in part for tok in toks):
            continue
        return domain, title
    return "", ""


def _resolve_chunk(
    chunk_row: dict[str, Any],
    *,
    unit: float,
    inputs: list[str],
    deadline: float | None = None,
) -> RowWorkResult:
    """One Apify chunk. chunk_row holds queries under _queries and a synthetic key."""
    queries: list[dict[str, str]] = list(chunk_row.get("_queries") or [])
    key = str(chunk_row.get("_source_key"))
    if not queries:
        return RowWorkResult(key=key, none=True, requests=0)
    run_id = _start_run(queries)
    items = _poll(run_id, n_queries=len(queries), deadline=deadline)
    by_query: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        search = _query_term(item)
        if search:
            by_query[search] = item
    # Pack per query outcomes into raw for the aggregator.
    packed: list[dict[str, Any]] = []
    none_n = 0
    hits_n = 0
    for q in queries:
        item = _item_for_query(by_query, q["q"])
        domain, title = (
            _domain_from_organic(item, company_name=q.get("name") or "")
            if isinstance(item, dict)
            else ("", "")
        )
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
    # Location is always on the SERP query when the row has it. geo_in_query does not apply.
    inputs = ["company_name", "city", "state"]
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
        city = str(row.get("city") or "").strip()
        state = str(row.get("state") or "").strip()
        q = build_serp_query(name, city, state)
        queries.append({"key": str(row.get("_source_key")), "q": q, "name": name})

    chunks = chunked(queries, SERP_CHUNK)
    chunk_rows: list[dict[str, Any]] = [
        {"_source_key": f"serp_chunk_{i}", "_queries": ch} for i, ch in enumerate(chunks)
    ]
    if concurrency is None and not (os.environ.get("SERP_TIER_CONCURRENCY") or "").strip():
        workers = SERP_DEFAULT_RUN_CONCURRENCY
    else:
        workers = resolve_tier_concurrency("serp", concurrency)

    def _one(chunk_row: dict[str, Any]) -> RowWorkResult:
        return _resolve_chunk(chunk_row, unit=unit, inputs=inputs, deadline=deadline)

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
