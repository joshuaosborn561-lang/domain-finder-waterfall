"""LeadMagic company-search is gone. Legacy names are a no-op with a warning."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from domain_waterfall.config import API_KEY_ALIASES, Settings, load_settings
from domain_waterfall.pricing import LEGACY_REMOVED_WARNING
from domain_waterfall.profiles import ClientProfile
from domain_waterfall.source import FetchResult
from domain_waterfall.vendors.base import TierResult
from domain_waterfall.waterfall import _run_tier, resolve_domain


def test_leadmagic_client_module_is_gone() -> None:
    with pytest.raises(ModuleNotFoundError):
        __import__("domain_waterfall.vendors.leadmagic")


def test_config_has_no_leadmagic_key() -> None:
    assert "leadmagic" not in API_KEY_ALIASES
    cfg = load_settings()
    assert not hasattr(cfg, "leadmagic_api_key")
    assert "leadmagic_api_key" not in Settings.__dataclass_fields__
    assert cfg.vendor_key("leadmagic") == ""


def test_run_tier_leadmagic_is_noop_without_http(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def boom(*_a: Any, **_k: Any) -> None:
        calls.append("http")
        raise AssertionError("LeadMagic HTTP must not run")

    monkeypatch.setattr("domain_waterfall.http_client.post", boom)
    monkeypatch.setattr("domain_waterfall.http_client.get", boom)
    result = _run_tier(
        "leadmagic",
        [{"_source_key": "1", "company_name": "Acme"}],
        ClientProfile("t"),
        with_location=True,
        units={},
        guessed={},
    )
    assert result.skipped == "removed_tier"
    assert result.candidates == {}
    assert result.cost_usd == 0
    assert calls == []


def _stub_legacy_profile(monkeypatch: pytest.MonkeyPatch, ran: list[str]) -> None:
    from domain_waterfall import waterfall as wf

    profile = ClientProfile(
        client_tag="goliath",
        raw={
            "enabled_tiers": ["cache", "maps", "leadmagic"],
            "tier_order": ["cache", "maps", "leadmagic"],
        },
    )
    monkeypatch.setattr(wf, "get_profile", lambda _tag: profile)
    monkeypatch.setattr(wf, "hydrate_keys", lambda: {"maps": True})
    monkeypatch.setattr(
        wf,
        "live_prices",
        lambda: ({"cache": 0.0, "maps": 0.0, "aiark": 0.0005}, {}),
    )
    fetched = FetchResult(
        rows=[{"_source_key": "1", "company_name": "Acme", "city": "Austin", "phone": ""}],
        rows_matched=1,
        rows_fetched=1,
        rows_excluded=0,
        exclusion_reasons={},
    )
    monkeypatch.setattr(wf, "fetch_source_rows", lambda _src: fetched)
    monkeypatch.setattr(wf, "count_source_rows", lambda _src: fetched.rows_matched)
    monkeypatch.setattr(wf, "ensure_writeback", lambda _src: None)
    monkeypatch.setattr(wf, "patch_source_row", lambda *_a, **_k: None)
    monkeypatch.setattr(wf, "defer_unfetched", lambda *_a, **_k: 0)

    def fake_run(name: str, rows: list[dict[str, Any]], *_a: Any, **_k: Any) -> TierResult:
        ran.append(name)
        return TierResult(tier=name, none=len(rows), rows_done=len(rows))

    monkeypatch.setattr(wf, "_run_tier", fake_run)


def test_profile_leadmagic_is_skipped_with_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[str] = []
    _stub_legacy_profile(monkeypatch, ran)
    out = resolve_domain(
        source_table="public.goliath_wf_companies",
        where="domain is null",
        client_tag="goliath",
        writeback=False,
    )
    assert ran == ["cache", "maps"]
    assert "leadmagic" not in ran
    assert LEGACY_REMOVED_WARNING in out["warnings"]
    assert all(t["tier"] != "leadmagic" for t in out["tiers"])


def test_request_max_tier_leadmagic_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[str] = []
    _stub_legacy_profile(monkeypatch, ran)
    out = resolve_domain(
        source_table="public.goliath_wf_companies",
        where="domain is null",
        client_tag="goliath",
        writeback=False,
        max_tier="leadmagic",
    )
    assert ran == ["cache", "maps"]
    assert LEGACY_REMOVED_WARNING in out["warnings"]


def test_request_min_tier_leadmagic_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[str] = []
    _stub_legacy_profile(monkeypatch, ran)
    out = resolve_domain(
        source_table="public.goliath_wf_companies",
        where="domain is null",
        client_tag="goliath",
        writeback=False,
        min_tier="leadmagic",
    )
    assert ran == ["cache", "maps"]
    assert LEGACY_REMOVED_WARNING in out["warnings"]


def test_request_skip_tiers_leadmagic_is_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[str] = []
    _stub_legacy_profile(monkeypatch, ran)
    out = resolve_domain(
        source_table="public.goliath_wf_companies",
        where="domain is null",
        client_tag="goliath",
        writeback=False,
        skip_tiers="leadmagic",
    )
    assert ran == ["cache", "maps"]
    assert LEGACY_REMOVED_WARNING in out["warnings"]


def test_strip_sql_is_profiles_only() -> None:
    sql = Path("supabase/migrations/005_strip_leadmagic_from_profiles.sql").read_text()
    assert "OPERATOR-RUN ONLY" in sql
    assert "Never touches dl_status, sg_exclude, or skip_*" in sql
    assert sql.count("UPDATE public.wf_client_profiles") == 1
    assert sql.upper().count("UPDATE ") == 1
    update = sql.split("UPDATE public.wf_client_profiles", 1)[1]
    assert "profile =" in update
    assert "updated_at" in update
    assert "dl_status" not in update
    assert "sg_exclude" not in update
    assert "skip_" not in update
