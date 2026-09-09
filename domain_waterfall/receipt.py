"""Phase-zero receipt test against profile ground_truth."""

from __future__ import annotations

import random
import re
from typing import Any, Callable

from .gate import evaluate
from .normalize import extract_domain, normalize_name
from .pricing import compute_order, estimate_rows
from .profiles import get_profile, update_profile_fields
from .source import fetch_source_rows, parse_source, where_to_filters
from .waterfall import _run_tier, live_prices

ProgressFn = Callable[[dict[str, Any]], None]


def _match(expected: str, got: str) -> bool:
    a = extract_domain(expected)
    b = extract_domain(got)
    return bool(a and b and a == b)


def _filter_receipt_rows(rows: list[dict[str, Any]], profile: Any) -> list[dict[str, Any]]:
    cities = {c.lower() for c in profile.cities}
    reject = profile.name_reject_regex
    compiled = re.compile(reject, re.I) if reject else None
    out = []
    for row in rows:
        name = str(row.get("company_name") or "")
        domain = extract_domain(str(row.get("domain") or row.get("wf_domain") or ""))
        if not name or not domain:
            continue
        city = str(row.get("city") or "").strip()
        if cities and city.lower() not in cities:
            continue
        if compiled and compiled.search(name):
            continue
        row = dict(row)
        row["_truth_domain"] = domain
        if not row.get("company_name_normalized"):
            row["company_name_normalized"] = normalize_name(name)
        out.append(row)
    return out


def receipt_test(
    client_tag: str,
    n: int = 25,
    *,
    progress: ProgressFn | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    from .waterfall import hydrate_keys

    hydrate_keys()
    profile = get_profile(client_tag)
    gt = profile.ground_truth
    table = str(gt.get("table") or "")
    if not table:
        raise ValueError(f"profile {client_tag} has no ground_truth.table")
    where = str(gt.get("where") or "")
    src = parse_source(table, where, limit=max(n * 80, 2000))
    src.column_map.setdefault("domain", "domain")
    raw_rows = fetch_source_rows(src)
    truth_rows = _attach_truth_domains(src, raw_rows)
    truth_rows = _filter_receipt_rows(truth_rows, profile)
    if not truth_rows:
        raise ValueError(
            f"no ground-truth rows after filters for {client_tag} on {src.qualified}"
        )
    sample = truth_rows[:]
    random.Random(42).shuffle(sample)
    sample = sample[:n]
    for i, row in enumerate(sample):
        row["_source_key"] = row.get("_source_key") if row.get("_source_key") is not None else i

    units, price_meta = live_prices()
    order = compute_order(
        profile.enabled_tiers,
        live_units=units,
        measured_hit_rates=profile.hit_rates,
        dropped=profile.dropped_tiers,
    )
    estimate = estimate_rows(len(sample), order)
    estimate["live_units"] = {k: order.prices[k].unit for k in order.tiers}
    estimate["price_meta"] = {
        k: {kk: vv for kk, vv in (v or {}).items() if kk not in {"account", "credits"}}
        for k, v in price_meta.items()
    }

    if progress:
        progress(
            {
                "status": "running",
                "phase": "estimate",
                "n": len(sample),
                "estimated_usd": estimate["estimated_usd"],
                "client_tag": profile.client_tag,
            }
        )

    def score_pass(with_location: bool) -> list[dict[str, Any]]:
        per_tier = []
        guessed: dict[str, str] = {}
        for tier_name in order.tiers:
            def _tick(processed: int, total: int, hits: int, *, _tier: str = tier_name) -> None:
                if progress:
                    progress(
                        {
                            "status": "running",
                            "phase": "tier",
                            "tier": _tier,
                            "processed": processed,
                            "targets": total,
                            "hits": hits,
                            "with_location": with_location,
                            "client_tag": profile.client_tag,
                        }
                    )

            result = _run_tier(
                tier_name,
                sample,
                profile,
                with_location=with_location,
                units=units,
                guessed=guessed,
                on_progress=_tick,
            )
            hits = wrong = none = 0
            for row in sample:
                key = str(row["_source_key"])
                cand = result.candidates.get(key)
                if not cand:
                    none += 1
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
                    none += 1
                    continue
                if _match(str(row.get("_truth_domain") or ""), gate.domain):
                    hits += 1
                    guessed[key] = gate.domain
                else:
                    wrong += 1
            rate = hits / len(sample) if sample else 0.0
            per_tier.append(
                {
                    "tier": tier_name,
                    "hits": hits,
                    "wrong": wrong,
                    "none": none,
                    "hit_rate": round(rate, 4),
                    "cost_usd": round(result.cost_usd, 6),
                    "credits": result.credits,
                    "inputs_passed": result.inputs_passed,
                    "skipped": result.skipped,
                    "error": result.error,
                    "with_location": with_location,
                }
            )
            if progress:
                progress(
                    {
                        "status": "running",
                        "phase": "tier",
                        "tier": tier_name,
                        "with_location": with_location,
                        "hits": hits,
                        "wrong": wrong,
                        "none": none,
                    }
                )
        return per_tier

    with_geo = score_pass(True)
    without_geo = score_pass(False)

    dropped = list(profile.dropped_tiers)
    hit_rates = dict(profile.hit_rates)
    for row in with_geo:
        if row.get("skipped") or row.get("error"):
            continue
        hit_rates[row["tier"]] = row["hit_rate"]
        if row["hits"] == 0 and row["tier"] not in dropped:
            dropped.append(row["tier"])

    new_order = compute_order(
        [t for t in profile.enabled_tiers if t not in dropped],
        live_units=units,
        measured_hit_rates=hit_rates,
        dropped=dropped,
    )
    if persist:
        update_profile_fields(
            profile.client_tag,
            {
                "dropped_tiers": dropped,
                "hit_rates": hit_rates,
                "tier_order": new_order.as_profile(),
            },
        )

    return {
        "ok": True,
        "client_tag": profile.client_tag,
        "n": len(sample),
        "ground_truth": {"table": src.qualified, "where": where},
        "estimate": estimate,
        "spent_usd": round(sum(t["cost_usd"] for t in with_geo + without_geo), 4),
        "with_location": with_geo,
        "without_location": without_geo,
        "dropped_tiers": dropped,
        "hit_rates": hit_rates,
        "tier_order": new_order.as_profile(),
    }


def _attach_truth_domains(src: Any, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    from . import supabase as sb

    cols = src.column_map
    # If fetch already has domain via accidental key, keep it.
    if rows and rows[0].get("domain"):
        return rows
    keys = [r.get("_source_key") for r in rows if r.get("_source_key") is not None]
    if not keys:
        return rows
    data = sb.rpc(
        "dw_read_source",
        {
            "p_schema": src.schema,
            "p_table": src.table,
            "p_filters": where_to_filters(src.where),
            "p_columns": [src.key_column, "domain", "website"],
            "p_key_column": src.key_column,
            "p_after": None,
            "p_limit": 500,
        },
    )
    blob = data if isinstance(data, list) else []
    if isinstance(data, dict):
        blob = data.get("dw_read_source") or []
    by_key = {
        str(r.get(src.key_column)): r
        for r in blob
        if isinstance(r, dict)
    }
    for row in rows:
        extra = by_key.get(str(row.get("_source_key")), {})
        row["domain"] = extra.get("domain") or extra.get("website")
    return rows
