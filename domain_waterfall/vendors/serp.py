"""Apify google search scraper. One async actor run per query batch, then poll."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
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
StatusFn = Callable[[str, str, dict[str, Any]], None]
SERP_CHUNK = 100
# A single google-search-scraper query is 45s plus. 120s was too short for a batch.
SERP_POLL_INTERVAL_S = 5.0
SERP_POLL_MIN_S = 900.0
SERP_POLL_PER_QUERY_S = 50.0
SERP_POLL_CAP_S = 45 * 60
SERP_START_TIMEOUT_S = 30
SERP_DEFAULT_RUN_CONCURRENCY = 2
SERP_MEMORY_MB = 4096
SERP_RUN_REUSE_S = 6 * 3600
RUNS_URL = "https://api.apify.com/v2/acts/{actor}/runs"
ALIVE_STATUSES = frozenset({"READY", "RUNNING"})
DONE_OK = "SUCCEEDED"
DONE_BAD = frozenset({"FAILED", "ABORTED", "TIMED-OUT"})


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


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


RUNS_DIR: Path | None = None


def _runs_dir() -> Path:
    path = RUNS_DIR if RUNS_DIR is not None else Path(__file__).resolve().parents[2] / "data" / "serp_runs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def chunk_fingerprint(queries: list[dict[str, str]]) -> str:
    blob = "\n".join(str(q.get("q") or "") for q in queries)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:24]


def _runs_url() -> str:
    # No waitForFinish: return runId immediately. A sync wait dies on a 30s HTTP timeout.
    # memory=4096: 100 queries were 368s with default memory; HTML snapshots fill KV.
    return f"{RUNS_URL.format(actor=_actor_id())}?memory={SERP_MEMORY_MB}"


def _run_url(run_id: str) -> str:
    rid = str(run_id or "").strip()
    if not rid or rid.lower() in {"last", "latest"} or "/" in rid or "?" in rid:
        raise VendorTransportError("serp", message="refusing to poll a non owned run id")
    return f"https://api.apify.com/v2/actor-runs/{rid}"


@dataclass
class SerpRunRecord:
    run_id: str
    chunk_key: str
    fingerprint: str
    query_count: int
    cost_usd: float
    queries: list[dict[str, str]] = field(default_factory=list)
    status: str = ""
    dataset_id: str = ""
    collected: bool = False
    started_at: float = field(default_factory=time.time)

    def public(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "chunk_key": self.chunk_key,
            "query_count": self.query_count,
            "cost_usd": round(self.cost_usd, 6),
            "status": self.status,
            "dataset_id": self.dataset_id,
            "collected": self.collected,
        }


class SerpRunTracker:
    """Own run ids for this job. Persist so stall/resume can collect, not re query."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.records: dict[str, SerpRunRecord] = {}

    def add(self, rec: SerpRunRecord) -> SerpRunRecord:
        with self._lock:
            self.records[rec.run_id] = rec
        _persist_run(rec)
        return rec

    def get(self, run_id: str) -> SerpRunRecord | None:
        with self._lock:
            return self.records.get(run_id)

    def owned(self, run_id: str) -> bool:
        with self._lock:
            return run_id in self.records

    def update(
        self,
        run_id: str,
        *,
        status: str = "",
        dataset_id: str = "",
        collected: bool | None = None,
    ) -> None:
        with self._lock:
            rec = self.records.get(run_id)
            if rec is None:
                return
            if status:
                rec.status = status
            if dataset_id:
                rec.dataset_id = dataset_id
            if collected is not None:
                rec.collected = collected
            snap = rec
        _persist_run(snap)

    def run_ids(self) -> list[str]:
        with self._lock:
            return list(self.records)

    def uncollected(self) -> list[SerpRunRecord]:
        with self._lock:
            return [r for r in self.records.values() if not r.collected]

    def cost_usd(self) -> float:
        with self._lock:
            return sum(r.cost_usd for r in self.records.values())

    def public(self) -> list[dict[str, Any]]:
        with self._lock:
            return [r.public() for r in self.records.values()]


def _persist_run(rec: SerpRunRecord) -> None:
    payload = {
        **rec.public(),
        "fingerprint": rec.fingerprint,
        "queries": rec.queries,
        "started_at": rec.started_at,
    }
    folder = _runs_dir()
    (folder / f"{rec.run_id}.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    (folder / f"fp-{rec.fingerprint}.json").write_text(
        json.dumps({"run_id": rec.run_id, "started_at": rec.started_at}),
        encoding="utf-8",
    )


def load_reusable_run(fingerprint: str) -> SerpRunRecord | None:
    path = _runs_dir() / f"fp-{fingerprint}.json"
    if not path.exists():
        return None
    try:
        idx = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    run_id = str(idx.get("run_id") or "")
    started = float(idx.get("started_at") or 0)
    if not run_id or time.time() - started > SERP_RUN_REUSE_S:
        return None
    raw_path = _runs_dir() / f"{run_id}.json"
    if not raw_path.exists():
        return None
    try:
        raw = json.loads(raw_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if str(raw.get("status") or "") in DONE_BAD:
        return None
    return SerpRunRecord(
        run_id=run_id,
        chunk_key=str(raw.get("chunk_key") or ""),
        fingerprint=fingerprint,
        query_count=int(raw.get("query_count") or 0),
        cost_usd=float(raw.get("cost_usd") or 0),
        queries=list(raw.get("queries") or []),
        status=str(raw.get("status") or ""),
        dataset_id=str(raw.get("dataset_id") or ""),
        collected=bool(raw.get("collected")),
        started_at=started,
    )


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
        "saveHtml": False,
        "saveHtmlToKeyValueStore": False,
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
    status = str((run or {}).get("status") or "")
    log.info("serp started run_id=%s queries=%s status=%s", run_id, len(queries), status)
    return run_id


def _response_run_id(body: dict[str, Any]) -> str:
    return str(body.get("id") or "").strip()


def _own_run_body(run_id: str, data: Any) -> dict[str, Any] | None:
    """Accept a poll payload only when it is this run_id. Never a list or last run."""
    if not isinstance(data, dict):
        return None
    body = data.get("data") if isinstance(data.get("data"), dict) else data
    if not isinstance(body, dict):
        return None
    if "items" in body and "id" not in body:
        log.warning("serp poll ignored run list wanted=%s", run_id)
        return None
    got = _response_run_id(body)
    if not got:
        log.warning("serp poll missing run id wanted=%s", run_id)
        return None
    if got != run_id:
        log.warning("serp poll ignored foreign run id=%s wanted=%s", got, run_id)
        return None
    return body


def _fetch_dataset(dataset_id: str) -> list[dict[str, Any]]:
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
    return items if isinstance(items, list) else []


def _get_own_run(run_id: str) -> dict[str, Any] | None:
    url = _run_url(run_id)
    try:
        r = http_client.get("serp", url, headers=_headers(), timeout=20)
    except VendorCallTimeout as exc:
        log.warning("serp poll http timeout run_id=%s, %s", run_id, exc)
        return None
    if r is None:
        return None
    if r.status_code == 429:
        raise VendorThrottle("serp", "poll 429")
    try:
        data = r.json()
    except ValueError:
        return None
    return _own_run_body(run_id, data)


def collect_run(run_id: str, *, tracker: SerpRunTracker | None = None) -> list[dict[str, Any]] | None:
    """Fetch dataset for an owned SUCCEEDED run. Used on stall and resume."""
    if tracker is not None and not tracker.owned(run_id):
        log.warning("serp collect refused foreign run id=%s", run_id)
        return None
    body = _get_own_run(run_id)
    if not body:
        return None
    status = str(body.get("status") or "")
    dataset_id = str(body.get("defaultDatasetId") or "")
    if tracker is not None:
        tracker.update(run_id, status=status, dataset_id=dataset_id)
    if status != DONE_OK:
        return None
    if not dataset_id:
        return None
    items = _fetch_dataset(dataset_id)
    if tracker is not None:
        tracker.update(run_id, status=DONE_OK, dataset_id=dataset_id, collected=True)
    log.info("serp run_id=%s status=%s items=%s", run_id, DONE_OK, len(items))
    return items


def _poll(
    run_id: str,
    *,
    n_queries: int,
    deadline: float | None = None,
    should_stop: StopFn | None = None,
    on_status: StatusFn | None = None,
    tracker: SerpRunTracker | None = None,
) -> list[dict[str, Any]]:
    if tracker is not None and not tracker.owned(run_id):
        raise VendorTransportError("serp", message="poll refused, run id is not owned")
    url = _run_url(run_id)
    budget = poll_budget_s(n_queries)
    started = time.monotonic()
    last_status = ""
    dataset_id = ""

    def _note(body: dict[str, Any]) -> None:
        nonlocal last_status, dataset_id
        last_status = str(body.get("status") or "")
        dataset_id = str(body.get("defaultDatasetId") or dataset_id)
        if tracker is not None:
            tracker.update(run_id, status=last_status, dataset_id=dataset_id)
        if last_status in ALIVE_STATUSES or last_status == DONE_OK:
            if on_status:
                on_status(run_id, last_status, body)

    while time.monotonic() - started < budget:
        if deadline is not None and time.monotonic() >= deadline:
            raise VendorTransportError(
                "serp",
                status=last_status or "deadline",
                message="job deadline during poll",
                timeout=round(time.monotonic() - started, 1),
                url=url,
            )
        stopping = bool(should_stop and should_stop())
        try:
            body = _get_own_run(run_id)
        except VendorThrottle:
            raise
        except VendorTransportError:
            raise
        if body:
            _note(body)
            if last_status == DONE_OK:
                break
            if last_status in DONE_BAD:
                raise VendorTransportError(
                    "serp",
                    status=last_status,
                    message=str(body.get("statusMessage") or last_status),
                    url=url,
                )
        if stopping:
            if last_status == DONE_OK:
                break
            raise VendorTransportError(
                "serp",
                status=last_status or "RUNNING",
                message="stopped during poll",
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
            status=last_status or DONE_OK,
            message="missing dataset id",
            url=url,
        )
    items = _fetch_dataset(dataset_id)
    if tracker is not None:
        tracker.update(run_id, status=DONE_OK, dataset_id=dataset_id, collected=True)
    log.info(
        "serp run_id=%s status=%s items=%s",
        run_id,
        last_status or DONE_OK,
        len(items),
    )
    return items


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


def _pack_items(
    queries: list[dict[str, str]],
    items: list[dict[str, Any]],
    *,
    unit: float,
) -> list[dict[str, Any]]:
    by_query: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        search = _query_term(item)
        if search:
            by_query[search] = item
    packed: list[dict[str, Any]] = []
    for q in queries:
        item = _item_for_query(by_query, q["q"])
        domain, title = (
            _domain_from_organic(item, company_name=q.get("name") or "")
            if isinstance(item, dict)
            else ("", "")
        )
        if not domain:
            packed.append({"key": q["key"], "none": True})
            continue
        packed.append(
            {
                "key": q["key"],
                "domain": domain,
                "title": title,
                "cost_usd": unit,
            }
        )
    return packed


def _ensure_run(
    chunk_row: dict[str, Any],
    queries: list[dict[str, str]],
    *,
    unit: float,
    tracker: SerpRunTracker,
) -> SerpRunRecord:
    """Start once. Resume and retries reuse the stored run id."""
    existing_id = str(chunk_row.get("_run_id") or "")
    if existing_id and tracker.owned(existing_id):
        rec = tracker.get(existing_id)
        if rec is not None:
            return rec
    fp = chunk_fingerprint(queries)
    reused = load_reusable_run(fp)
    if reused is not None:
        reused.chunk_key = str(chunk_row.get("_source_key") or reused.chunk_key)
        reused.queries = queries
        reused.query_count = len(queries)
        if reused.cost_usd <= 0:
            reused.cost_usd = unit * len(queries)
        chunk_row["_run_id"] = reused.run_id
        tracker.add(reused)
        log.info("serp reuse run_id=%s queries=%s", reused.run_id, len(queries))
        return reused
    run_id = _start_run(queries)
    rec = SerpRunRecord(
        run_id=run_id,
        chunk_key=str(chunk_row.get("_source_key") or ""),
        fingerprint=fp,
        query_count=len(queries),
        cost_usd=unit * len(queries),
        queries=queries,
        status="READY",
    )
    chunk_row["_run_id"] = run_id
    tracker.add(rec)
    return rec


def _resolve_chunk(
    chunk_row: dict[str, Any],
    *,
    unit: float,
    inputs: list[str],
    deadline: float | None = None,
    should_stop: StopFn | None = None,
    on_status: StatusFn | None = None,
    tracker: SerpRunTracker | None = None,
) -> RowWorkResult:
    """One Apify chunk. chunk_row holds queries under _queries and a synthetic key."""
    queries: list[dict[str, str]] = list(chunk_row.get("_queries") or [])
    key = str(chunk_row.get("_source_key"))
    if not queries:
        return RowWorkResult(key=key, none=True, requests=0)
    store = tracker or SerpRunTracker()
    rec = _ensure_run(chunk_row, queries, unit=unit, tracker=store)
    if on_status:
        on_status(rec.run_id, rec.status or "READY", {})
    items = _poll(
        rec.run_id,
        n_queries=len(queries),
        deadline=deadline,
        should_stop=should_stop,
        on_status=on_status,
        tracker=store,
    )
    store.update(rec.run_id, status=DONE_OK, collected=True)
    packed = _pack_items(queries, items, unit=unit)
    none_n = sum(1 for p in packed if p.get("none"))
    hits_n = len(packed) - none_n
    carrier = DomainCandidate(
        domain="",
        inputs_passed=inputs,
        billed=True,
        cost_usd=rec.cost_usd,
        credits=0.0,
        raw={
            "serp_chunk": packed,
            "none": none_n,
            "hits": hits_n,
            "queries": len(queries),
            "run_id": rec.run_id,
        },
    )
    return RowWorkResult(
        key=key,
        candidate=carrier,
        none=False,
        requests=2 + len(queries),
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

    tracker = SerpRunTracker()
    # Cost is billed when a run starts, not when the dataset lands.
    billed_started = 0.0

    def heartbeat(run_id: str = "", status: str = "", _body: dict[str, Any] | None = None) -> None:
        report_progress(
            on_progress,
            0,
            len(rows),
            0,
            {
                "requests_made": 0,
                "errored": 0,
                "last_progress_at": _utc_now_iso(),
                "serp_run_ids": tracker.run_ids(),
                "serp_runs": tracker.public(),
                "tier_cost_usd": tracker.cost_usd(),
                "serp_poll_run_id": run_id,
                "serp_poll_status": status,
            },
        )

    def _one(chunk_row: dict[str, Any]) -> RowWorkResult:
        return _resolve_chunk(
            chunk_row,
            unit=unit,
            inputs=inputs,
            deadline=deadline,
            should_stop=should_stop,
            on_status=heartbeat,
            tracker=tracker,
        )

    report_progress(
        on_progress,
        0,
        len(rows),
        0,
        {
            "requests_made": 0,
            "errored": 0,
            "last_progress_at": _utc_now_iso(),
            "serp_run_ids": [],
            "serp_runs": [],
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

    # Stall/resume: collect finished datasets for runs we started, never start again.
    harvested: dict[str, list[dict[str, Any]]] = {}
    for rec in tracker.uncollected():
        try:
            items = collect_run(rec.run_id, tracker=tracker)
        except (VendorTransportError, VendorThrottle) as exc:
            log.warning("serp harvest run_id=%s failed, %s", rec.run_id, exc)
            continue
        if items is None:
            continue
        harvested[rec.chunk_key] = items

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
        _apply_packed((carrier.raw or {}).get("serp_chunk") or [])
    by_chunk = {str(row.get("_source_key")): row for row in chunk_rows}
    for chunk_key, items in harvested.items():
        qs = list((by_chunk.get(chunk_key) or {}).get("_queries") or [])
        _apply_packed(_pack_items(qs, items, unit=unit))

    # Started runs are billed even when the job stalls before unpack.
    final.cost_usd = tracker.cost_usd()
    billed_started = final.cost_usd
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
            "serp_run_ids": tracker.run_ids(),
            "serp_runs": tracker.public(),
            "tier_cost_usd": billed_started,
        },
    )
    return final
