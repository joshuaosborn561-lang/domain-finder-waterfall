"""Domain waterfall. Never finds a person. Never finds an email."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable

from . import supabase as sb
from .config import load_settings
from .gate import evaluate, sink_map
from .normalize import extract_domain, normalize_name
from .pricing import DEFAULT_PIPELINE, compute_order, estimate_rows
from .profiles import ClientProfile, get_profile
from .source import (
    TableSource,
    defer_unfetched,
    ensure_writeback,
    fetch_source_rows,
    parse_source,
    patch_source_row,
)
from .vendors import aiark, cache, discolike, leadmagic, maps, prospeo, serp
from .vendors.base import DomainCandidate, OnProgress, TierResult

ProgressFn = Callable[[dict[str, Any]], None]
StopFn = Callable[[], bool]

TIER_MIN_BUDGET_S = 600.0
TIER_PER_ROW_S = 2.0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_skip_tiers(raw: Any) -> list[str]:
    if raw is None or raw is False:
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        if text.startswith("["):
            import json

            try:
                return parse_skip_tiers(json.loads(text))
            except ValueError:
                pass
        return [p.strip().lower() for p in text.split(",") if p.strip()]
    if isinstance(raw, (list, tuple, set)):
        return [str(x).strip().lower() for x in raw if str(x).strip()]
    return []


def apply_tier_filters(
    tiers: list[str],
    *,
    min_tier: str = "",
    skip_tiers: Any = None,
) -> list[str]:
    """Drop skipped names and everything before min_tier in the current order."""
    out = list(tiers)
    skip = set(parse_skip_tiers(skip_tiers))
    mt = (min_tier or "").strip().lower()
    if mt:
        if mt not in out:
            raise ValueError(f"min_tier {mt!r} is not in the planned tier order {out}")
        out = out[out.index(mt) :]
    return [t for t in out if t not in skip]


def tier_budget_seconds(n_rows: int) -> float:
    return max(TIER_MIN_BUDGET_S, max(0, int(n_rows)) * TIER_PER_ROW_S)


def make_row_ticker(
    emit: Callable[[dict[str, Any]], None],
    *,
    min_interval_s: float = 2.0,
    clock: Callable[[], float] = time.monotonic,
) -> OnProgress:
    """Emit in-tier progress. Always on first and last row; otherwise throttle."""
    last = 0.0
    started = False

    def tick(
        processed: int,
        total: int,
        hits: int,
        extra: dict[str, Any] | None = None,
    ) -> None:
        nonlocal last, started
        now = clock()
        done = total > 0 and processed >= total
        if started and not done and now - last < min_interval_s:
            return
        started = True
        last = now
        extra = extra or {}
        emit(
            {
                "processed": processed,
                "targets": total,
                "hits": hits,
                "rows_attempted": extra.get("rows_attempted", processed),
                "rows_done": extra.get("rows_done", processed),
                "accepted": extra.get("accepted", hits),
                "requests_made": extra.get("requests_made", 0),
                "last_progress_at": utc_now_iso(),
            }
        )

    return tick

FREE_ALWAYS = frozenset({"cache", "maps"})
FREE_ON_MISS = frozenset({"prospeo"})
PAID = frozenset({"aiark", "discolike", "serp", "leadmagic"})


def load_private_keys() -> dict[str, str]:
    try:
        data = sb.rpc("dw_api_keys", {})
    except RuntimeError:
        return {}
    rows = data if isinstance(data, list) else []
    if isinstance(data, dict):
        rows = data.get("dw_api_keys") or []
    out: dict[str, str] = {}
    for row in rows:
        if isinstance(row, dict) and row.get("name") and row.get("value"):
            out[str(row["name"]).strip().lower()] = str(row["value"]).strip()
    return out


def hydrate_keys() -> dict[str, bool]:
    import os

    extra = load_private_keys()
    env_map = {
        "discolike": "DISCOLIKE_API_KEY",
        "aiark": "AI_ARK_API_KEY",
        "ai_ark": "AI_ARK_API_KEY",
        "leadmagic": "LEADMAGIC_API_KEY",
        "prospeo": "PROSPEO_API_KEY",
        "apify": "APIFY_TOKEN",
        "rapidapi": "RAPIDAPI_KEY",
    }
    for name, env_name in env_map.items():
        val = extra.get(name)
        if val and not os.environ.get(env_name):
            os.environ[env_name] = val
    import domain_waterfall.config as cfgmod

    cfg = cfgmod.load_settings()
    cfg.extra_keys = extra
    cfgmod.settings = cfg
    maps.settings = cfg
    aiark.settings = cfg
    discolike.settings = cfg
    prospeo.settings = cfg
    leadmagic.settings = cfg
    serp.settings = cfg
    return {
        "discolike": bool(cfg.discolike_api_key),
        "aiark": bool(cfg.ai_ark_api_key),
        "leadmagic": bool(cfg.leadmagic_api_key),
        "prospeo": bool(cfg.prospeo_api_key),
        "serp": bool(cfg.apify_token),
        "maps": bool(cfg.rapidapi_key),
    }


def live_prices() -> tuple[dict[str, float], dict[str, Any]]:
    hydrate_keys()
    units: dict[str, float] = {
        "cache": 0.0,
        "maps": 0.0,
        "discolike": 0.00425,
    }
    meta: dict[str, Any] = {}
    ai_unit, ai_info = aiark.live_unit_price()
    units["aiark"] = ai_unit
    meta["aiark"] = ai_info
    serp_unit, serp_info = serp.live_unit_price()
    units["serp"] = serp_unit
    meta["serp"] = serp_info
    pr_unit, pr_info = prospeo.live_unit_price()
    units["prospeo"] = pr_unit
    meta["prospeo"] = pr_info
    lm_unit, lm_info = leadmagic.live_unit_price()
    units["leadmagic"] = lm_unit
    meta["leadmagic"] = lm_info
    return units, meta


def _run_tier(
    name: str,
    rows: list[dict[str, Any]],
    profile: ClientProfile,
    *,
    with_location: bool,
    units: dict[str, float],
    guessed: dict[str, str],
    on_progress: OnProgress | None = None,
    deadline: float | None = None,
    should_stop: StopFn | None = None,
) -> TierResult:
    if name == "cache":
        return cache.lookup_many(
            rows, extra_tables=profile.cache_tables, on_progress=on_progress
        )
    if name == "maps":
        return maps.resolve_rows(
            rows,
            with_location=with_location,
            on_progress=on_progress,
            deadline=deadline,
            should_stop=should_stop,
        )
    if name == "aiark":
        return aiark.resolve_rows(
            rows,
            profile,
            with_location=with_location,
            unit=units.get("aiark", 0.0005),
            on_progress=on_progress,
        )
    if name == "discolike":
        return discolike.resolve_rows(
            rows, with_location=True, on_progress=on_progress
        )
    if name == "serp":
        return serp.resolve_rows(
            rows,
            with_location=with_location,
            unit=units.get("serp", 0.0045),
            on_progress=on_progress,
        )
    if name == "prospeo":
        return prospeo.resolve_rows(
            rows,
            guessed=guessed,
            unit=units.get("prospeo", 0.015),
            on_progress=on_progress,
        )
    if name == "leadmagic":
        return leadmagic.resolve_rows(
            rows, unit=units.get("leadmagic", 0.015), on_progress=on_progress
        )
    out = TierResult(tier=name)
    out.skipped = "unknown_tier"
    return out


def _should_run_paid(
    row_state: dict[str, Any],
    profile: ClientProfile,
    tier: str,
) -> bool:
    status = row_state.get("status")
    if status in (None, "", "domain_unresolved", "deferred"):
        return True
    if status == "review":
        return True
    if profile.second_opinion:
        return True
    return False


def resolve_domain(
    *,
    source_table: str,
    where: str,
    client_tag: str,
    max_tier: str = "",
    min_tier: str = "",
    skip_tiers: Any = None,
    approve_cost_usd: float | None = None,
    estimate_only: bool = False,
    with_location: bool = True,
    progress: ProgressFn | None = None,
    writeback: bool = True,
    limit: int | None = None,
    should_stop: StopFn | None = None,
) -> dict[str, Any]:
    hydrate_keys()
    profile = get_profile(client_tag)
    src = parse_source(source_table, where, limit=limit)
    units, price_meta = live_prices()
    order = compute_order(
        profile.enabled_tiers,
        live_units=units,
        measured_hit_rates=profile.hit_rates,
        dropped=profile.dropped_tiers,
        explicit_order=profile.explicit_tier_order,
    )
    if max_tier:
        stop = max_tier.strip().lower()
        if stop in order.tiers:
            order.tiers = order.tiers[: order.tiers.index(stop) + 1]

    fetched = fetch_source_rows(src)
    census = fetched.to_public()
    rows = fetched.rows
    n = len(rows)
    estimate = estimate_rows(n, order)
    estimate["live_units"] = {k: order.prices[k].unit for k in order.tiers}
    estimate["price_meta"] = {
        k: {kk: vv for kk, vv in (price_meta.get(k) or {}).items() if kk != "account"}
        for k in price_meta
    }
    estimate["tier_order"] = order.as_profile()
    estimate["client_tag"] = profile.client_tag
    estimate["source_table"] = src.qualified
    estimate["where"] = src.where
    estimate.update(census)
    if estimate_only:
        return {"ok": True, "estimate_only": True, **estimate}

    # min_tier / skip_tiers apply to the real run only. Estimate stays complete.
    order.tiers = apply_tier_filters(
        order.tiers, min_tier=min_tier, skip_tiers=skip_tiers
    )

    if writeback:
        ensure_writeback(src)
        if fetched.exclusion_reasons.get("job_limit"):
            deferred_n = defer_unfetched(src, [str(r["_source_key"]) for r in rows])
            census = {**census, "deferred_unfetched": deferred_n}

    query_location = profile.geo_in_query
    pending = {str(r["_source_key"]): r for r in rows}
    states: dict[str, dict[str, Any]] = {
        str(r["_source_key"]): {
            "status": None,
            "domain": None,
            "source": None,
            "confidence": 0.0,
            "agreement": False,
            "candidates": [],
            "phone": r.get("phone") or "",
            "company_name": r.get("company_name"),
            "city": r.get("city"),
        }
        for r in rows
    }
    spent = 0.0
    deferred_tier = ""
    tier_stats: list[dict[str, Any]] = []
    guessed: dict[str, str] = {}

    def stopped() -> bool:
        return bool(should_stop and should_stop())

    def emit(extra: dict[str, Any] | None = None) -> None:
        if not progress:
            return
        snap = {
            "status": "running",
            "client_tag": profile.client_tag,
            "source_table": src.qualified,
            "rows": n,
            **census,
            "spent_usd": round(spent, 4),
            "resolved": sum(1 for s in states.values() if s["status"] == "resolved"),
            "review": sum(1 for s in states.values() if s["status"] == "review"),
            "deferred": sum(1 for s in states.values() if s["status"] == "deferred"),
            "unresolved": sum(1 for s in states.values() if s["status"] in (None, "domain_unresolved")),
            "tiers": tier_stats,
            "last_progress_at": utc_now_iso(),
        }
        if extra:
            snap.update(extra)
        if "last_progress_at" not in (extra or {}):
            snap["last_progress_at"] = utc_now_iso()
        progress(snap)

    emit({"phase": "start", "estimate_usd": estimate["estimated_usd"]})

    cancelled = False
    for tier_name in order.tiers:
        if stopped():
            cancelled = True
            emit({"phase": "cancelled", "error": "cancelled", "status": "failed"})
            break
        price = order.prices[tier_name]
        is_paid = (not price.free) and price.unit > 0 and not price.free_on_miss
        is_free_on_miss = price.free_on_miss
        always = tier_name in FREE_ALWAYS or is_free_on_miss

        targets: list[dict[str, Any]] = []
        for key, row in pending.items():
            st = states[key]
            if always:
                if st["status"] == "deferred":
                    continue
                targets.append(row)
                continue
            if is_paid and not _should_run_paid(st, profile, tier_name):
                continue
            if st["status"] in ("resolved", "review") and not profile.second_opinion:
                continue
            targets.append(row)

        if not targets:
            continue

        projected = spent + (price.unit * len(targets) if is_paid else 0.0)
        if is_paid and approve_cost_usd is not None and projected > approve_cost_usd:
            deferred_tier = tier_name
            for row in targets:
                key = str(row["_source_key"])
                if states[key]["status"] in (None, "domain_unresolved"):
                    states[key]["status"] = "deferred"
            emit({"phase": "cost_gate", "stopped_before": tier_name, "would_cost_usd": round(projected, 4)})
            break

        budget_s = tier_budget_seconds(len(targets))
        deadline = time.monotonic() + budget_s
        emit(
            {
                "phase": "tier",
                "tier": tier_name,
                "targets": len(targets),
                "processed": 0,
                "hits": 0,
                "rows_attempted": 0,
                "rows_done": 0,
                "accepted": 0,
                "requests_made": 0,
                "tier_budget_s": budget_s,
            }
        )
        ticker = make_row_ticker(
            lambda extra, _tier=tier_name: emit({"phase": "tier", "tier": _tier, **extra})
        )
        result = _run_tier(
            tier_name,
            targets,
            profile,
            with_location=query_location,
            units=units,
            guessed=guessed,
            on_progress=ticker,
            deadline=deadline,
            should_stop=lambda d=deadline: stopped() or time.monotonic() >= d,
        )
        if not result.error and time.monotonic() >= deadline:
            result.error = "tier timeout"
        spent += result.cost_usd

        accepted_rows: list[dict[str, Any]] = []
        hits = wrong = 0  # populated by receipt; here just accepted/rejected
        accepted = 0
        rejected = 0
        for row in targets:
            key = str(row["_source_key"])
            cand = result.candidates.get(key)
            if not cand:
                continue
            gate = evaluate(
                profile.gate(),
                input_name=str(row.get("company_name") or ""),
                domain=cand.domain,
                vendor_name=cand.vendor_name,
                title=cand.title,
                phone=cand.phone or "",
                address_state=cand.address_state or "",
            )
            if not gate.accepted:
                rejected += 1
                continue
            accepted += 1
            accepted_rows.append(
                {
                    "key": key,
                    "domain": gate.domain,
                    "company_name": row.get("company_name"),
                    "gate": gate,
                    "phone": cand.phone,
                    "tier": tier_name,
                }
            )

        sinks = sink_map(
            [{"domain": a["domain"], "company_name": a["company_name"]} for a in accepted_rows]
        )
        for item in accepted_rows:
            if item["domain"] in sinks:
                rejected += 1
                accepted -= 1
                continue
            key = item["key"]
            gate = item["gate"]
            st = states[key]
            domain = item["domain"]
            guessed[key] = domain
            if item["phone"]:
                st["phone"] = item["phone"]
            existing = st.get("domain")
            if existing and extract_domain(existing) == domain and st.get("source") != tier_name:
                st["agreement"] = True
                st["confidence"] = 0.95
                st["status"] = "resolved"
                st["source"] = f"{st['source']}+{tier_name}"
            elif existing and extract_domain(existing) != domain:
                st["status"] = "review"
                st["candidates"] = [
                    {"domain": existing, "source": st.get("source"), "confidence": st.get("confidence")},
                    {"domain": domain, "source": tier_name, "confidence": gate.confidence},
                ]
                st["domain"] = None
                st["confidence"] = 0.0
            else:
                st["domain"] = domain
                st["source"] = tier_name
                st["confidence"] = gate.confidence
                st["status"] = gate.status

        tier_stats.append(
            {
                "tier": tier_name,
                "targets": len(targets),
                "accepted": accepted,
                "rejected_by_gate": rejected,
                "none": result.none,
                "cost_usd": round(result.cost_usd, 6),
                "credits": result.credits,
                "inputs_passed": result.inputs_passed,
                "skipped": result.skipped,
                "error": result.error,
                "rows_attempted": len(targets),
                "rows_done": result.rows_done or (0 if result.error == "tier timeout" else len(targets)),
                "requests_made": result.calls,
                "cache_key": "company_name_normalized" if tier_name == "cache" else None,
            }
        )
        emit(
            {
                "phase": "tier_done",
                "tier": tier_name,
                "rows_attempted": len(targets),
                "rows_done": result.rows_done,
                "accepted": accepted,
                "requests_made": result.calls,
            }
        )

    counts = {
        "resolved": 0,
        "review": 0,
        "deferred": 0,
        "domain_unresolved": 0,
        "agreement": 0,
        "at_or_above_0_7": 0,
    }
    write_i = 0
    for key, st in states.items():
        if stopped():
            cancelled = True
            break
        if st["status"] is None:
            st["status"] = "domain_unresolved"
        counts[st["status"]] = counts.get(st["status"], 0) + 1
        if st.get("agreement"):
            counts["agreement"] += 1
        if (st.get("confidence") or 0) >= 0.7 and st["status"] == "resolved":
            counts["at_or_above_0_7"] += 1
        write_i += 1
        if writeback and (write_i == 1 or write_i % 50 == 0 or write_i == n):
            emit({"phase": "writeback", "processed": write_i, "targets": n})
        if writeback:
            fields = {
                "wf_domain": st.get("domain"),
                "wf_domain_source": st.get("source"),
                "wf_domain_confidence": st.get("confidence") or None,
                "wf_domain_agreement": bool(st.get("agreement")),
                "wf_domain_candidates": st.get("candidates") or None,
                "wf_phone": st.get("phone") or None,
                "wf_domain_status": st["status"],
            }
            if st["status"] == "review" and st.get("candidates"):
                fields["wf_domain"] = None
            if (st.get("confidence") or 0) < 0.5 and st["status"] not in (
                "review",
                "deferred",
                "domain_unresolved",
            ):
                continue
            patch_source_row(src, key, fields)
            if st.get("domain") and st["status"] == "resolved":
                cache.remember(
                    str(st.get("company_name") or ""),
                    str(st["domain"]),
                    str(st.get("source") or ""),
                    profile.client_tag,
                )

    next_tier = deferred_tier or ""
    if not next_tier:
        remaining = [t for t in DEFAULT_PIPELINE if t not in order.tiers and t not in profile.dropped_tiers]
        next_tier = remaining[0] if remaining else ""

    summary = {
        "ok": not cancelled,
        "estimate_only": False,
        "client_tag": profile.client_tag,
        "source_table": src.qualified,
        "where": src.where,
        "rows": n,
        **census,
        "spent_usd": round(spent, 4),
        "approve_cost_usd": approve_cost_usd,
        "counts": counts,
        "agreement_rate": round(counts["agreement"] / n, 4) if n else 0.0,
        "tiers": tier_stats,
        "tier_order": order.as_profile(),
        "unresolved": counts["domain_unresolved"],
        "next_tier": next_tier,
        "deferred_stopped_before": deferred_tier or None,
        "live_units": {k: order.prices[k].unit for k in order.tiers},
    }
    if cancelled:
        summary["error"] = "cancelled"
        emit({**summary, "status": "failed"})
    else:
        emit({**summary, "status": "completed"})
    return summary
