from urllib.error import URLError

import pytest

from domain_waterfall.source import (
    WRITEBACK_MAX_ATTEMPTS,
    WritebackResult,
    is_transient_writeback_error,
    parse_source,
    patch_source_rows,
    preflight_write_columns,
    writeback_rpc,
)
from domain_waterfall.source import FetchResult  # noqa: F401


def _fields() -> dict:
    return {
        "wf_domain": "x.com",
        "wf_domain_source": "maps",
        "wf_domain_confidence": 0.85,
        "wf_domain_agreement": False,
        "wf_domain_candidates": None,
        "wf_phone": None,
        "wf_domain_status": "resolved",
    }


def test_patch_source_rows_1200_three_batches_partial_save(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    saved: list[str] = []

    def fake_rpc(name: str, body: dict | None = None) -> int:
        assert name == "dw_patch_source_batch"
        rows = (body or {}).get("p_rows") or []
        keys = [str(r["id"]) for r in rows]
        calls.append(keys)
        if any(500 <= int(k) < 1000 for k in keys):
            raise RuntimeError("batch 2 failed")
        saved.extend(keys)
        return len(keys)

    monkeypatch.setattr("domain_waterfall.source.sb.rpc", fake_rpc)
    src = parse_source("public.t")
    items = [(i, _fields()) for i in range(1200)]
    result = patch_source_rows(src, items, concurrency=4)
    assert len(calls) == 3
    assert sorted(len(c) for c in calls) == [200, 500, 500]
    written = {int(k) for k in result.written_keys}
    failed = {int(k) for k, _ in result.failed_items}
    assert written == set(range(500)) | set(range(1000, 1200))
    assert failed == set(range(500, 1000))
    assert {int(k) for k in saved} == written


def test_preflight_integer_agreement_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "domain_waterfall.source.fetch_column_types",
        lambda _src: {
            "wf_domain_agreement": "integer",
            "wf_domain_confidence": "numeric",
            "wf_domain_candidates": "jsonb",
        },
    )
    src = parse_source("public.t")
    with pytest.raises(ValueError, match="wf_domain_agreement") as excinfo:
        preflight_write_columns(src)
    assert "integer" in str(excinfo.value)


def test_batch_rpc_missing_falls_back_to_per_row(monkeypatch: pytest.MonkeyPatch) -> None:
    per_row: list[str] = []

    def fake_rpc(name: str, body: dict | None = None) -> None:
        if name == "dw_patch_source_batch":
            raise RuntimeError(
                "Supabase POST rpc/dw_patch_source_batch failed (404): "
                "Could not find the function public.dw_patch_source_batch"
            )
        raise AssertionError(f"unexpected rpc {name}")

    def fake_row(_src: object, key: object, _fields: object) -> None:
        per_row.append(str(key))

    monkeypatch.setattr("domain_waterfall.source.sb.rpc", fake_rpc)
    monkeypatch.setattr("domain_waterfall.source.patch_source_row", fake_row)
    src = parse_source("public.t")
    items = [(i, {"wf_domain_status": "resolved"}) for i in range(3)]
    result = patch_source_rows(src, items, concurrency=1)
    assert result.written_keys == [0, 1, 2]
    assert result.failed_items == []
    assert per_row == ["0", "1", "2"]


def test_transient_writeback_then_success(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fake_rpc(name: str, body: dict | None = None) -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise URLError("SSLV3_ALERT_HANDSHAKE_FAILURE")
        return 1

    monkeypatch.setattr("domain_waterfall.source.sb.rpc", fake_rpc)
    monkeypatch.setattr("domain_waterfall.source._writeback_sleep", sleeps.append)
    src = parse_source("public.t")
    result = patch_source_rows(src, [(1, _fields())], concurrency=1)
    assert result.written_keys == ["1"]
    assert result.failed_items == []
    assert calls["n"] == 2
    assert sleeps == [2.0]


def test_persistent_writeback_does_not_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fake_rpc(name: str, body: dict | None = None) -> int:
        calls["n"] += 1
        raise RuntimeError("Supabase POST rpc/dw_patch_source_batch failed (503): unavailable")

    monkeypatch.setattr("domain_waterfall.source.sb.rpc", fake_rpc)
    monkeypatch.setattr("domain_waterfall.source._writeback_sleep", sleeps.append)
    src = parse_source("public.t")
    result = patch_source_rows(src, [(1, _fields())], concurrency=1)
    assert result.written_keys == []
    assert [k for k, _ in result.failed_items] == [1]
    assert calls["n"] == WRITEBACK_MAX_ATTEMPTS
    assert sleeps == [2.0, 5.0, 15.0]


def test_is_transient_writeback_error() -> None:
    assert is_transient_writeback_error(URLError("timed out"))
    assert is_transient_writeback_error(TimeoutError("timeout"))
    assert is_transient_writeback_error(ConnectionResetError())
    assert is_transient_writeback_error(
        RuntimeError("Supabase POST failed (502): bad gateway")
    )
    assert is_transient_writeback_error(
        RuntimeError("SSLV3_ALERT_HANDSHAKE_FAILURE")
    )
    assert not is_transient_writeback_error(RuntimeError("batch 2 failed"))
    assert not is_transient_writeback_error(ValueError("wf_domain_agreement"))


def test_writeback_rpc_retries_then_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def fake_rpc(name: str, body: dict | None = None) -> None:
        calls["n"] += 1
        raise URLError("timed out")

    monkeypatch.setattr("domain_waterfall.source.sb.rpc", fake_rpc)
    monkeypatch.setattr("domain_waterfall.source._writeback_sleep", sleeps.append)
    with pytest.raises(URLError):
        writeback_rpc("dw_patch_source", {"p_key": "1"})
    assert calls["n"] == WRITEBACK_MAX_ATTEMPTS
    assert sleeps == [2.0, 5.0, 15.0]


def test_persistent_writeback_does_not_kill_job(monkeypatch: pytest.MonkeyPatch) -> None:
    from typing import Any

    from domain_waterfall.profiles import ClientProfile
    from domain_waterfall.source import FetchResult
    from domain_waterfall.vendors.base import DomainCandidate, TierResult
    from domain_waterfall.waterfall import resolve_domain

    from domain_waterfall import waterfall as wf

    profile = ClientProfile(
        client_tag="t",
        raw={"enabled_tiers": ["cache"], "geo": {"geo_required": False}},
    )
    monkeypatch.setattr(wf, "get_profile", lambda _tag: profile)
    monkeypatch.setattr(wf, "hydrate_keys", lambda: {})
    monkeypatch.setattr(wf, "live_prices", lambda: ({"cache": 0.0, "maps": 0.0}, {}))
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
    monkeypatch.setattr(wf, "defer_unfetched", lambda *_a, **_k: 0)
    monkeypatch.setattr(wf, "preflight_write_columns", lambda *_a, **_k: None)

    def always_fail(_src: Any, items: list, **_k: Any) -> WritebackResult:
        return WritebackResult(written_keys=[], failed_items=list(items))

    monkeypatch.setattr(wf, "patch_source_rows", always_fail)
    monkeypatch.setattr(
        wf,
        "_run_tier",
        lambda name, rows, *_a, **_k: TierResult(
            tier=name,
            candidates={"1": DomainCandidate(domain="acme.com", title="Acme")},
            rows_done=len(rows),
        ),
    )
    out = resolve_domain(
        source_table="public.t",
        where="domain is null",
        client_tag="t",
        writeback=True,
        max_tier="cache",
    )
    assert out["ok"] is True
    assert out["failed_writeback_rows"] >= 1
    assert out["counts"]["failed_writeback_rows"] >= 1
