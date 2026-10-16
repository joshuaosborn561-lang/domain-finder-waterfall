from typing import Any

import pytest

from domain_waterfall.profiles import ClientProfile
from domain_waterfall.source import FetchResult
from domain_waterfall.concurrency import VendorThrottle
from domain_waterfall.vendors.base import DomainCandidate, TierResult
from domain_waterfall.waterfall import (
    apply_tier_filters,
    parse_skip_tiers,
    resolve_domain,
    tier_budget_seconds,
)


def test_parse_and_apply_filters() -> None:
    assert parse_skip_tiers("maps, aiark") == ["maps", "aiark"]
    assert parse_skip_tiers(["maps"]) == ["maps"]
    tiers = ["cache", "maps", "aiark", "discolike"]
    assert apply_tier_filters(tiers, min_tier="aiark") == ["aiark", "discolike"]
    assert apply_tier_filters(tiers, skip_tiers="maps") == ["cache", "aiark", "discolike"]
    assert apply_tier_filters(tiers, min_tier="aiark", skip_tiers=["aiark"]) == ["discolike"]
    with pytest.raises(ValueError):
        apply_tier_filters(tiers, min_tier="nope")
    assert apply_tier_filters(tiers, min_tier="leadmagic") == tiers
    assert apply_tier_filters(["cache", "leadmagic", "maps"]) == ["cache", "maps"]


def test_tier_budget_floor() -> None:
    assert tier_budget_seconds(10) == 600
    assert tier_budget_seconds(400) == 800


def _stub_resolve(monkeypatch: pytest.MonkeyPatch, ran: list[str]) -> None:
    from domain_waterfall import waterfall as wf

    profile = ClientProfile(
        client_tag="t",
        raw={"enabled_tiers": ["cache", "maps", "aiark"]},
    )
    monkeypatch.setattr(wf, "get_profile", lambda _tag: profile)
    monkeypatch.setattr(wf, "hydrate_keys", lambda: {"maps": True, "aiark": True})
    monkeypatch.setattr(
        wf,
        "live_prices",
        lambda: (
            {
                "cache": 0.0,
                "maps": 0.0,
                "aiark": 0.0005,
                "discolike": 0.00425,
                "serp": 0.0045,
                "prospeo": 0.015,
            },
            {},
        ),
    )
    fetched = FetchResult(
        rows=[
            {
                "_source_key": "1",
                "company_name": "Acme",
                "city": "Austin",
                "phone": "",
            }
        ],
        rows_matched=1,
        rows_fetched=1,
        rows_excluded=0,
        exclusion_reasons={},
    )
    monkeypatch.setattr(wf, "fetch_source_rows", lambda _src: fetched)
    monkeypatch.setattr(wf, "count_source_rows", lambda _src: fetched.rows_matched)
    monkeypatch.setattr(wf, "ensure_writeback", lambda _src: None)
    monkeypatch.setattr(wf, "patch_source_row", lambda *_a, **_k: None)
    monkeypatch.setattr(wf, "patch_source_rows", lambda *_a, **_k: [])
    monkeypatch.setattr(wf, "preflight_write_columns", lambda *_a, **_k: None)
    monkeypatch.setattr(wf, "defer_unfetched", lambda *_a, **_k: 0)

    def fake_run(name: str, rows: list[dict[str, Any]], *_a: Any, **_k: Any) -> TierResult:
        ran.append(name)
        if name == "maps":
            return TierResult(tier="maps", none=len(rows), error="tier timeout", rows_done=0)
        return TierResult(tier=name, none=len(rows), rows_done=len(rows))

    monkeypatch.setattr(wf, "_run_tier", fake_run)


def test_maps_timeout_advances_to_next_tier(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[str] = []
    _stub_resolve(monkeypatch, ran)
    out = resolve_domain(
        source_table="public.t",
        where="domain is null",
        client_tag="t",
        writeback=False,
        max_tier="aiark",
    )
    assert ran == ["cache", "maps", "aiark"]
    maps_stat = next(t for t in out["tiers"] if t["tier"] == "maps")
    assert maps_stat["error"] == "tier timeout"
    assert out["ok"] is True


def test_skip_tiers_on_real_run(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[str] = []
    _stub_resolve(monkeypatch, ran)
    out = resolve_domain(
        source_table="public.t",
        where="domain is null",
        client_tag="t",
        writeback=False,
        max_tier="aiark",
        skip_tiers="maps",
    )
    assert ran == ["cache", "aiark"]
    assert all(t["tier"] != "maps" for t in out["tiers"])


def test_min_tier_on_real_run(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[str] = []
    _stub_resolve(monkeypatch, ran)
    out = resolve_domain(
        source_table="public.t",
        where="domain is null",
        client_tag="t",
        writeback=False,
        max_tier="aiark",
        min_tier="aiark",
    )
    assert ran == ["aiark"]
    assert out["ok"] is True


def test_estimate_only_logs_sql_and_tier_plan(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import logging

    ran: list[str] = []
    _stub_resolve(monkeypatch, ran)
    caplog.set_level(logging.INFO, logger="domain_waterfall")
    out = resolve_domain(
        source_table="public.t",
        where="domain is null",
        client_tag="t",
        estimate_only=True,
        max_tier="aiark",
    )
    assert out["estimate_only"] is True
    assert out["source_sql"] == "SELECT count(*) FROM public.t WHERE true AND domain IS NULL"
    assert "tier_plan" in caplog.text or "cache" in caplog.text
    assert "SELECT count(*) FROM public.t" in caplog.text
    assert out["rows_matched"] == 1
    assert ran == []


def test_estimate_only_ignores_skip_and_min(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[str] = []
    _stub_resolve(monkeypatch, ran)
    out = resolve_domain(
        source_table="public.t",
        where="domain is null",
        client_tag="t",
        estimate_only=True,
        max_tier="aiark",
        min_tier="aiark",
        skip_tiers="maps",
    )
    assert out["estimate_only"] is True
    assert ran == []
    names = [t["tier"] for t in out["tiers"]]
    assert names == ["cache", "maps", "aiark"]


def test_in_tier_progress_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    from domain_waterfall import waterfall as wf

    ran: list[str] = []
    _stub_resolve(monkeypatch, ran)
    snaps: list[dict[str, Any]] = []

    def fake_run(name: str, rows: list[dict[str, Any]], *_a: Any, **kwargs: Any) -> TierResult:
        ran.append(name)
        on_progress = kwargs.get("on_progress")
        if on_progress:
            on_progress(1, 1, 0, {"requests_made": 2, "rows_done": 1, "rows_attempted": 1})
        return TierResult(tier=name, none=len(rows), rows_done=1, calls=2)

    monkeypatch.setattr(wf, "_run_tier", fake_run)
    resolve_domain(
        source_table="public.t",
        where="domain is null",
        client_tag="t",
        writeback=False,
        max_tier="cache",
        progress=snaps.append,
    )
    tier_snaps = [s for s in snaps if s.get("phase") == "tier"]
    assert tier_snaps
    last = tier_snaps[-1]
    assert "last_progress_at" in last
    assert last["rows_attempted"] == 1
    assert last["rows_done"] == 1
    assert last["requests_made"] == 2


def test_writeback_flushes_after_each_tier(monkeypatch: pytest.MonkeyPatch) -> None:
    from domain_waterfall import waterfall as wf
    from domain_waterfall.vendors import cache as cache_mod

    profile = ClientProfile(
        client_tag="t",
        raw={
            "enabled_tiers": ["cache", "prospeo"],
            "geo": {"geo_required": False},
        },
    )
    monkeypatch.setattr(wf, "get_profile", lambda _tag: profile)
    monkeypatch.setattr(wf, "hydrate_keys", lambda: {"prospeo": True})
    monkeypatch.setattr(
        wf,
        "live_prices",
        lambda: ({"cache": 0.0, "maps": 0.0, "prospeo": 0.015}, {}),
    )
    fetched = FetchResult(
        rows=[
            {
                "_source_key": "1",
                "company_name": "Acme",
                "city": "Austin",
                "phone": "",
            }
        ],
        rows_matched=1,
        rows_fetched=1,
        rows_excluded=0,
        exclusion_reasons={},
    )
    monkeypatch.setattr(wf, "fetch_source_rows", lambda _src: fetched)
    monkeypatch.setattr(wf, "count_source_rows", lambda _src: fetched.rows_matched)
    monkeypatch.setattr(wf, "ensure_writeback", lambda _src: None)
    monkeypatch.setattr(wf, "defer_unfetched", lambda *_a, **_k: 0)
    monkeypatch.setattr(wf, "preflight_write_columns", lambda *_a, **_k: None)
    monkeypatch.setattr(cache_mod, "remember", lambda *_a, **_k: None)

    patches: list[tuple[str, dict[str, Any]]] = []

    def capture_rows(_src: Any, items: list, **_k: Any) -> list:
        for key, fields in items:
            patches.append((key, dict(fields)))
        return [key for key, _ in items]

    monkeypatch.setattr(wf, "patch_source_rows", capture_rows)

    def fake_run(name: str, rows: list[dict[str, Any]], *_a: Any, **_k: Any) -> TierResult:
        if name == "prospeo":
            raise VendorThrottle("prospeo", "http 429")
        if name == "cache":
            return TierResult(
                tier="cache",
                candidates={"1": DomainCandidate(domain="acme.com", title="Acme")},
                rows_done=len(rows),
            )
        return TierResult(tier=name, none=len(rows), rows_done=len(rows))

    monkeypatch.setattr(wf, "_run_tier", fake_run)
    with pytest.raises(VendorThrottle, match="http 429"):
        resolve_domain(
            source_table="public.t",
            where="domain is null",
            client_tag="t",
            writeback=True,
            max_tier="prospeo",
        )
    assert patches
    assert patches[0][0] == "1"
    assert patches[0][1]["wf_domain"] == "acme.com"
    assert patches[0][1]["wf_domain_source"] == "cache"
    assert patches[0][1]["wf_domain_status"] == "resolved"


def test_unresolved_keeps_gate_rejected_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    from domain_waterfall import waterfall as wf

    profile = ClientProfile(
        client_tag="t",
        raw={
            "enabled_tiers": ["cache"],
            "geo": {"geo_required": False, "states": ["TX"]},
            "name_strip_tokens": ["inc", "llc", "construction"],
        },
    )
    monkeypatch.setattr(wf, "get_profile", lambda _tag: profile)
    monkeypatch.setattr(wf, "hydrate_keys", lambda: {})
    monkeypatch.setattr(wf, "live_prices", lambda: ({"cache": 0.0, "maps": 0.0}, {}))
    fetched = FetchResult(
        rows=[
            {
                "_source_key": "1",
                "company_name": "Smith Roofing",
                "city": "Dallas",
                "state": "TX",
                "phone": "",
            }
        ],
        rows_matched=1,
        rows_fetched=1,
        rows_excluded=0,
        exclusion_reasons={},
    )
    monkeypatch.setattr(wf, "fetch_source_rows", lambda _src: fetched)
    monkeypatch.setattr(wf, "count_source_rows", lambda _src: fetched.rows_matched)
    monkeypatch.setattr(wf, "ensure_writeback", lambda _src: None)
    monkeypatch.setattr(wf, "defer_unfetched", lambda *_a, **_k: 0)
    monkeypatch.setattr(wf, "preflight_write_columns", lambda *_a, **_k: None)

    patches: list[tuple[str, dict[str, Any]]] = []

    def capture_rows(_src: Any, items: list, **_k: Any) -> list:
        for key, fields in items:
            patches.append((key, dict(fields)))
        return [key for key, _ in items]

    monkeypatch.setattr(wf, "patch_source_rows", capture_rows)

    def fake_run(name: str, rows: list[dict[str, Any]], *_a: Any, **_k: Any) -> TierResult:
        return TierResult(
            tier=name,
            candidates={"1": DomainCandidate(domain="yelp.com", title="Smith Roofing, Yelp")},
            rows_done=len(rows),
        )

    monkeypatch.setattr(wf, "_run_tier", fake_run)
    out = resolve_domain(
        source_table="public.t",
        where="domain is null",
        client_tag="t",
        writeback=True,
        max_tier="cache",
    )
    assert out["counts"]["domain_unresolved"] == 1
    assert out["rejected_by_gate"].get("blocklist") == 1
    assert patches
    last = patches[-1][1]
    assert last["wf_domain_status"] == "domain_unresolved"
    cands = last["wf_domain_candidates"]
    assert isinstance(cands, list) and cands
    assert cands[0]["domain"] == "yelp.com"
    assert cands[0]["reason"] == "blocklist"
    assert cands[0]["tier"] == "cache"
    assert "title" in cands[0]
    assert "vendor_name" in cands[0]
    assert "address_city" in cands[0]
    assert "address_state" in cands[0]
    assert "phone" in cands[0]
    banned = {"dl_status", "sg_exclude"}
    assert banned.isdisjoint(last)
    assert all(not k.startswith("skip_") for k in last)
