"""Serper google search. One synchronous batch POST per query chunk."""

from __future__ import annotations

import logging
import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Callable

from domain_waterfall import http_client
from domain_waterfall.config import settings
from domain_waterfall.gate import ProfileGate, evaluate, is_blocklisted
from domain_waterfall.normalize import extract_domain
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

StopFn = Callable[[], bool]
SERP_CHUNK = 100
SERP_URL = "https://google.serper.dev/search"
SERP_TIMEOUT_S = 60
SERP_HTTP_RETRIES = 3
SERP_DEFAULT_CHUNK_CONCURRENCY = 4
DEFAULT_UNIT_USD = 0.001


def _headers() -> dict[str, str]:
    return {
        "X-API-KEY": settings.serper_api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def live_unit_price() -> tuple[float, dict[str, Any]]:
    info: dict[str, Any] = {
        "configured": bool(settings.serper_api_key),
        "billing": "serper google search",
    }
    unit = DEFAULT_UNIT_USD
    raw = (os.environ.get("SERPER_UNIT_USD") or "").strip()
    if raw:
        try:
            unit = float(raw)
        except ValueError:
            pass
    info["unit_usd"] = unit
    info["source"] = "env" if raw else "published"
    return unit, info


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _retry_delay(attempt: int) -> float:
    return min(30.0, (2**attempt) + random.uniform(0, 0.5))


def _search_batch(queries: list[dict[str, str]]) -> list[dict[str, Any]]:
    """One Serper POST for a chunk. Retry 429 and 5xx three times, then raise."""
    url = SERP_URL
    body = [{"q": q["q"], "gl": "us", "hl": "en", "num": 10} for q in queries]
    last_error: Exception | None = None
    for attempt in range(SERP_HTTP_RETRIES + 1):
        try:
            r = http_client.post(
                "serp",
                url,
                json=body,
                headers=_headers(),
                timeout=SERP_TIMEOUT_S,
                max_attempts=1,
            )
        except VendorCallTimeout as exc:
            last_error = VendorTransportError(
                "serp",
                status="timeout",
                message=str(exc),
                timeout=float(SERP_TIMEOUT_S),
                url=url,
            )
            if attempt < SERP_HTTP_RETRIES:
                time.sleep(_retry_delay(attempt))
                continue
            raise last_error from exc
        except VendorThrottle as exc:
            last_error = exc
            if attempt < SERP_HTTP_RETRIES:
                time.sleep(_retry_delay(attempt))
                continue
            raise
        except VendorTransportError as exc:
            last_error = exc
            if attempt < SERP_HTTP_RETRIES:
                time.sleep(_retry_delay(attempt))
                continue
            raise
        if r is None:
            last_error = VendorTransportError(
                "serp",
                status="none",
                message="search returned no response",
                timeout=float(SERP_TIMEOUT_S),
                url=url,
            )
            if attempt < SERP_HTTP_RETRIES:
                time.sleep(_retry_delay(attempt))
                continue
            raise last_error
        if r.status_code == 429:
            last_error = VendorThrottle("serp", "http 429")
            if attempt < SERP_HTTP_RETRIES:
                time.sleep(_retry_delay(attempt))
                continue
            raise last_error
        if r.status_code >= 500:
            last_error = VendorTransportError(
                "serp",
                status=r.status_code,
                message=f"http {r.status_code}",
                url=url,
            )
            if attempt < SERP_HTTP_RETRIES:
                time.sleep(_retry_delay(attempt))
                continue
            raise last_error
        if r.status_code >= 400:
            snippet = ""
            try:
                snippet = (r.text or "")[:300]
            except Exception:
                snippet = ""
            raise VendorTransportError(
                "serp",
                status=r.status_code,
                message=snippet or f"http {r.status_code}",
                url=url,
            )
        try:
            data = r.json()
        except ValueError as exc:
            raise VendorTransportError("serp", message="bad json", url=url) from exc
        if isinstance(data, list):
            return [item if isinstance(item, dict) else {} for item in data]
        if isinstance(data, dict):
            return [data]
        raise VendorTransportError("serp", message="unexpected response", url=url)
    raise last_error or VendorTransportError(
        "serp",
        status="none",
        message="search failed",
        url=url,
    )


def _query_term(item: dict[str, Any]) -> str:
    params = item.get("searchParameters")
    if isinstance(params, dict):
        q = str(params.get("q") or "").strip()
        if q:
            return q
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


SERP_GATE_TOP_N = 5


def _profile_gate(profile: Any) -> ProfileGate:
    if isinstance(profile, ProfileGate):
        return profile
    if profile is not None and hasattr(profile, "gate"):
        try:
            return profile.gate()
        except Exception:
            pass
    return ProfileGate()


def _domain_from_organic(
    item: dict[str, Any],
    company_name: str = "",
    *,
    profile: Any = None,
    city: str = "",
    input_state: str = "",
    max_results: int = SERP_GATE_TOP_N,
) -> tuple[str, str, list[dict[str, Any]]]:
    """Walk organic hits in order. Gate each non-blocklisted result; try the next on reject."""
    organic = item.get("organicResults") or item.get("organic") or []
    if not isinstance(organic, list):
        return "", "", []
    gate = _profile_gate(profile)
    rejected: list[dict[str, Any]] = []
    considered = 0
    for hit in organic:
        if not isinstance(hit, dict):
            continue
        url = str(hit.get("url") or hit.get("link") or "")
        domain = extract_domain(url)
        if not domain or is_blocklisted(domain, gate.blocklist):
            continue
        title = str(hit.get("title") or "")
        considered += 1
        if considered > max_results:
            break
        result = evaluate(
            gate,
            input_name=company_name,
            domain=domain,
            title=title,
            city=city,
            input_state=input_state,
        )
        if result.accepted:
            return domain, title, rejected
        rejected.append(
            {
                "domain": result.domain or domain,
                "tier": "serp",
                "title": title,
                "reason": result.reason,
            }
        )
    return "", "", rejected


def _pack_items(
    queries: list[dict[str, str]],
    items: list[dict[str, Any]],
    *,
    unit: float,
    profile: Any = None,
) -> list[dict[str, Any]]:
    by_query: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        search = _query_term(item)
        if search:
            by_query[search] = item
    packed: list[dict[str, Any]] = []
    for i, q in enumerate(queries):
        item: dict[str, Any] | None = None
        if i < len(items) and isinstance(items[i], dict):
            item = items[i]
        else:
            item = _item_for_query(by_query, q["q"])
        domain, title, rejected = (
            _domain_from_organic(
                item,
                company_name=q.get("name") or "",
                profile=profile,
                city=q.get("city") or "",
                input_state=q.get("state") or "",
            )
            if isinstance(item, dict)
            else ("", "", [])
        )
        if not domain:
            row: dict[str, Any] = {"key": q["key"], "none": True}
            if rejected:
                row["rejected"] = rejected
            packed.append(row)
            continue
        packed_row: dict[str, Any] = {
            "key": q["key"],
            "domain": domain,
            "title": title,
            "cost_usd": unit,
        }
        if rejected:
            packed_row["rejected"] = rejected
        packed.append(packed_row)
    return packed


def _resolve_chunk(
    chunk_row: dict[str, Any],
    *,
    unit: float,
    inputs: list[str],
    profile: Any = None,
) -> RowWorkResult:
    """One Serper chunk. chunk_row holds queries under _queries and a synthetic key."""
    queries: list[dict[str, str]] = list(chunk_row.get("_queries") or [])
    key = str(chunk_row.get("_source_key"))
    if not queries:
        return RowWorkResult(key=key, none=True, requests=0)
    items = _search_batch(queries)
    packed = _pack_items(queries, items, unit=unit, profile=profile)
    none_n = sum(1 for p in packed if p.get("none"))
    hits_n = len(packed) - none_n
    cost = unit * len(queries)
    carrier = DomainCandidate(
        domain="",
        inputs_passed=inputs,
        billed=True,
        cost_usd=cost,
        credits=0.0,
        raw={
            "serp_chunk": packed,
            "none": none_n,
            "hits": hits_n,
            "queries": len(queries),
        },
    )
    return RowWorkResult(
        key=key,
        candidate=carrier,
        none=False,
        requests=1,
    )


def resolve_rows(
    rows: list[dict[str, Any]],
    *,
    with_location: bool = True,
    unit: float = DEFAULT_UNIT_USD,
    on_progress: OnProgress | None = None,
    deadline: float | None = None,
    should_stop: StopFn | None = None,
    concurrency: int | None = None,
    profile: Any = None,
) -> TierResult:
    # Location is always on the SERP query when the row has it. geo_in_query does not apply.
    inputs = ["company_name", "city", "state"]
    result = TierResult(
        tier="serp",
        inputs_passed=inputs,
        billing="serper google search",
    )
    if not settings.serper_api_key:
        result.skipped = "serper_key_missing"
        return result
    # Build query snapshot from the fixed row list. Never re query source.
    queries: list[dict[str, str]] = []
    for row in rows:
        name = str(row.get("company_name") or "").strip()
        city = str(row.get("city") or "").strip()
        state = str(row.get("state") or "").strip()
        q = build_serp_query(name, city, state)
        queries.append(
            {
                "key": str(row.get("_source_key")),
                "q": q,
                "name": name,
                "city": city,
                "state": state,
            }
        )

    chunks = chunked(queries, SERP_CHUNK)
    chunk_rows: list[dict[str, Any]] = [
        {"_source_key": f"serp_chunk_{i}", "_queries": ch} for i, ch in enumerate(chunks)
    ]
    if concurrency is None and not (os.environ.get("SERP_TIER_CONCURRENCY") or "").strip():
        workers = SERP_DEFAULT_CHUNK_CONCURRENCY
    else:
        workers = resolve_tier_concurrency("serp", concurrency)

    def _one(chunk_row: dict[str, Any]) -> RowWorkResult:
        return _resolve_chunk(chunk_row, unit=unit, inputs=inputs, profile=profile)

    report_progress(
        on_progress,
        0,
        len(rows),
        0,
        {
            "requests_made": 0,
            "errored": 0,
            "last_progress_at": _utc_now_iso(),
            "tier_cost_usd": 0.0,
        },
    )
    pooled = run_row_pool(
        chunk_rows,
        _one,
        tier="serp",
        concurrency=workers,
        on_progress=None,
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
    seen_keys: set[str] = set()
    done_rows = 0

    def _apply_packed(packed: list[Any]) -> None:
        nonlocal done_rows
        for item in packed:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key"))
            if not key or key in seen_keys:
                continue
            seen_keys.add(key)
            rejected = item.get("rejected") or []
            if isinstance(rejected, list) and rejected:
                final.rejected[key] = [r for r in rejected if isinstance(r, dict)]
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
            final.billed_calls += 1
            done_rows += 1

    for carrier in pooled.candidates.values():
        final.cost_usd += carrier.cost_usd
        _apply_packed((carrier.raw or {}).get("serp_chunk") or [])

    # Chunks that errored after retries: count leftover queries as errored, not none.
    errored_chunks = pooled.errored
    if errored_chunks:
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
            "last_progress_at": _utc_now_iso(),
            "tier_cost_usd": final.cost_usd,
        },
    )
    return final
