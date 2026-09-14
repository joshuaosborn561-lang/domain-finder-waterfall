from typing import Any

import pytest

from domain_waterfall.profiles import ClientProfile
from domain_waterfall.source import FetchResult
from domain_waterfall.vendors.base import TierResult
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
                "leadmagic": 0.015,
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
    monkeypatch.setattr(wf, "ensure_writeback", lambda _src: None)
    monkeypatch.setattr(wf, "patch_source_row", lambda *_a, **_k: None)
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
