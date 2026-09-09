"""Local cache: prior wf_domain at the same normalized name, plus profile cache_tables."""

from __future__ import annotations

from typing import Any

from domain_waterfall import supabase as sb
from domain_waterfall.normalize import extract_domain, normalize_name
from domain_waterfall.vendors.base import DomainCandidate, OnProgress, TierResult, report_progress


def lookup_cache(names: list[str], extra_tables: list[str] | None = None) -> TierResult:
    result = TierResult(tier="cache", inputs_passed=["company_name_normalized"])
    wanted = [normalize_name(n) for n in names if n]
    if not wanted:
        return result
    data = sb.rpc(
        "dw_cache_lookup",
        {"p_names": wanted, "p_tables": extra_tables or []},
    )
    rows = data if isinstance(data, list) else []
    if isinstance(data, dict):
        rows = data.get("dw_cache_lookup") or data.get("data") or []
    by_name: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = normalize_name(str(row.get("company_name_normalized") or row.get("name") or ""))
        domain = extract_domain(str(row.get("wf_domain") or row.get("domain") or ""))
        if name and domain:
            by_name[name] = domain
    for raw_name in names:
        key = normalize_name(raw_name)
        domain = by_name.get(key)
        if not domain:
            result.none += 1
            continue
        result.candidates[raw_name] = DomainCandidate(
            domain=domain,
            vendor_name=raw_name,
            inputs_passed=["company_name_normalized"],
        )
    result.calls = 1
    return result


def remember(name: str, domain: str, source: str, client_tag: str) -> None:
    domain = extract_domain(domain)
    key = normalize_name(name)
    if not domain or not key:
        return
    try:
        sb.rpc(
            "dw_cache_remember",
            {
                "p_name": key,
                "p_domain": domain,
                "p_source": source,
                "p_client_tag": client_tag,
            },
        )
    except RuntimeError:
        return


def lookup_many(
    rows: list[dict[str, Any]],
    extra_tables: list[str] | None = None,
    on_progress: OnProgress | None = None,
) -> TierResult:
    # Cache key is normalized company name only. City/state are never part of the join.
    names = [str(r.get("company_name") or "") for r in rows]
    report_progress(on_progress, 0, len(rows), 0)
    keyed = lookup_cache(names, extra_tables=extra_tables)
    by_norm: dict[str, DomainCandidate] = {}
    for raw_name, cand in keyed.candidates.items():
        by_norm[normalize_name(raw_name)] = cand
    out = TierResult(tier="cache", inputs_passed=["company_name_normalized"])
    for row in rows:
        key = str(row.get("_source_key"))
        norm = normalize_name(str(row.get("company_name") or ""))
        cand = by_norm.get(norm)
        if not cand:
            out.none += 1
            continue
        out.candidates[key] = cand
    out.calls = keyed.calls
    report_progress(on_progress, len(rows), len(rows), len(out.candidates))
    return out
