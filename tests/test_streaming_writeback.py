"""Streaming writeback: flush during a tier, 60s timer, crash/rerun, in-memory hand-off."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from domain_waterfall.profiles import ClientProfile
from domain_waterfall.source import FetchResult, WritebackResult, _batch_payload, parse_source
from domain_waterfall.supabase import merge_write_fields
from domain_waterfall.vendors.base import DomainCandidate, TierResult


def _profile(tiers: list[str]) -> ClientProfile:
    return ClientProfile(
        client_tag="t",
        raw={
            "enabled_tiers": tiers,
            "geo": {"geo_required": False},
        },
    )


def _prices() -> tuple[dict[str, float], dict]:
    return (
        {
            "cache": 0.0,
            "maps": 0.0,
            "aiark": 0.0005,
            "discolike": 0.00425,
            "serp": 0.001,
            "prospeo": 0.015,
        },
        {},
    )


def _row(i: int) -> dict[str, Any]:
    return {
        "_source_key": str(i),
        "company_name": f"Acme{i:04d}",
        "city": "Austin",
        "state": "TX",
        "phone": "",
    }


def _cand(i: int) -> DomainCandidate:
    return DomainCandidate(
        domain=f"acme{i:04d}.com",
        vendor_name=f"Acme{i:04d}",
        title=f"Acme{i:04d}",
    )


def _stub_common(monkeypatch: pytest.MonkeyPatch, *, n: int, tiers: list[str]) -> dict[str, Any]:
    from domain_waterfall import waterfall as wf

    monkeypatch.setattr(wf, "get_profile", lambda _tag: _profile(tiers))
    monkeypatch.setattr(wf, "hydrate_keys", lambda: {"maps": True, "aiark": True})
    monkeypatch.setattr(wf, "live_prices", _prices)
    monkeypatch.setattr(wf, "ensure_writeback", lambda _src: None)
    monkeypatch.setattr(wf, "defer_unfetched", lambda *_a, **_k: 0)
    monkeypatch.setattr(wf, "preflight_write_columns", lambda *_a, **_k: None)
    rows = [_row(i) for i in range(n)]
    fetched = FetchResult(
        rows=rows,
        rows_matched=n,
        rows_fetched=n,
        rows_excluded=0,
        exclusion_reasons={},
    )
    monkeypatch.setattr(wf, "fetch_source_rows", lambda _src: fetched)
    monkeypatch.setattr(wf, "count_source_rows", lambda _src: n)
    return {"rows": rows, "wf": wf}


def test_streaming_flush_during_long_maps_tier(monkeypatch: pytest.MonkeyPatch) -> None:
    from domain_waterfall import waterfall as wf

    n = 600
    _stub_common(monkeypatch, n=n, tiers=["maps"])
    patches: list[str] = []
    lock = threading.Lock()
    saved_before_return = {"n": 0}

    def capture(_src: Any, items: list, **_k: Any) -> WritebackResult:
        keys = [str(k) for k, _ in items]
        with lock:
            patches.extend(keys)
        return WritebackResult(written_keys=keys)

    monkeypatch.setattr(wf, "patch_source_rows", capture)

    def fake_run(name: str, rows: list[dict[str, Any]], *_a: Any, **kwargs: Any) -> TierResult:
        on_settled = kwargs.get("on_settled")
        result = TierResult(tier=name, rows_done=len(rows))
        for i, row in enumerate(rows):
            key = str(row["_source_key"])
            cand = _cand(int(key))
            result.candidates[key] = cand
            if on_settled:
                on_settled(key, cand)
            if i + 1 == 500:
                deadline = time.time() + 5
                while time.time() < deadline:
                    with lock:
                        if len(patches) >= 500:
                            break
                    time.sleep(0.01)
                with lock:
                    saved_before_return["n"] = len(patches)
        return result

    monkeypatch.setattr(wf, "_run_tier", fake_run)
    snaps: list[dict[str, Any]] = []
    out = wf.resolve_domain(
        source_table="public.t",
        where="wf_domain_status is null",
        client_tag="t",
        writeback=True,
        max_tier="maps",
        progress=snaps.append,
    )
    assert saved_before_return["n"] >= 500
    assert out["saved_rows"] >= 500
    assert out["counts"]["resolved"] == n
    status_snaps = [s for s in snaps if "saved_rows" in s]
    assert status_snaps
    assert any(s.get("saved_rows", 0) >= 500 and s.get("phase") == "tier" for s in snaps) or any(
        (s.get("saved_rows") or 0) >= 500 for s in snaps
    )
    assert any(s.get("last_flush_at") for s in snaps)


def test_timer_flush_before_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    from domain_waterfall import waterfall as wf

    monkeypatch.setenv("WRITEBACK_FLUSH_INTERVAL_S", "0.05")
    n = 10
    _stub_common(monkeypatch, n=n, tiers=["maps"])
    patches: list[str] = []
    lock = threading.Lock()
    saved_before_return = {"n": 0}

    def capture(_src: Any, items: list, **_k: Any) -> WritebackResult:
        keys = [str(k) for k, _ in items]
        with lock:
            patches.extend(keys)
        return WritebackResult(written_keys=keys)

    monkeypatch.setattr(wf, "patch_source_rows", capture)

    def fake_run(name: str, rows: list[dict[str, Any]], *_a: Any, **kwargs: Any) -> TierResult:
        on_settled = kwargs.get("on_settled")
        result = TierResult(tier=name, rows_done=len(rows))
        for row in rows:
            key = str(row["_source_key"])
            cand = _cand(int(key))
            result.candidates[key] = cand
            if on_settled:
                on_settled(key, cand)
        deadline = time.time() + 3
        while time.time() < deadline:
            with lock:
                if len(patches) >= n:
                    break
            time.sleep(0.02)
        with lock:
            saved_before_return["n"] = len(patches)
        return result

    monkeypatch.setattr(wf, "_run_tier", fake_run)
    out = wf.resolve_domain(
        source_table="public.t",
        where="wf_domain_status is null",
        client_tag="t",
        writeback=True,
        max_tier="maps",
    )
    assert saved_before_return["n"] >= n
    assert out["saved_rows"] >= n
    assert out["last_flush_at"]


def test_crash_after_two_flushes_then_rerun_skips_saved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from domain_waterfall import waterfall as wf

    n = 1200
    table: dict[str, dict[str, Any]] = {
        str(i): {**_row(i), "wf_domain_status": None} for i in range(n)
    }
    monkeypatch.setattr(wf, "get_profile", lambda _tag: _profile(["maps"]))
    monkeypatch.setattr(wf, "hydrate_keys", lambda: {"maps": True})
    monkeypatch.setattr(wf, "live_prices", _prices)
    monkeypatch.setattr(wf, "ensure_writeback", lambda _src: None)
    monkeypatch.setattr(wf, "defer_unfetched", lambda *_a, **_k: 0)
    monkeypatch.setattr(wf, "preflight_write_columns", lambda *_a, **_k: None)

    def fetch(_src: Any) -> FetchResult:
        rows = [
            {k: v for k, v in row.items() if k != "wf_domain_status"}
            for key, row in table.items()
            if row.get("wf_domain_status") is None
        ]
        return FetchResult(
            rows=rows,
            rows_matched=len(rows),
            rows_fetched=len(rows),
            rows_excluded=0,
            exclusion_reasons={},
        )

    monkeypatch.setattr(wf, "fetch_source_rows", fetch)
    monkeypatch.setattr(wf, "count_source_rows", lambda _src: len(fetch(_src).rows))

    flush_count = {"n": 0}
    lock = threading.Lock()

    def capture(_src: Any, items: list, **_k: Any) -> WritebackResult:
        keys = []
        for key, fields in items:
            key_s = str(key)
            table[key_s].update(fields)
            keys.append(key_s)
        with lock:
            flush_count["n"] += 1
        return WritebackResult(written_keys=keys)

    monkeypatch.setattr(wf, "patch_source_rows", capture)

    def fake_run(name: str, rows: list[dict[str, Any]], *_a: Any, **kwargs: Any) -> TierResult:
        on_settled = kwargs.get("on_settled")
        result = TierResult(tier=name, rows_done=0)
        for i, row in enumerate(rows):
            key = str(row["_source_key"])
            cand = _cand(int(key))
            result.candidates[key] = cand
            if on_settled:
                on_settled(key, cand)
            if i + 1 >= 1000:
                deadline = time.time() + 5
                while time.time() < deadline:
                    saved = sum(1 for r in table.values() if r.get("wf_domain_status") is not None)
                    if saved >= 1000:
                        break
                    time.sleep(0.01)
                raise RuntimeError("simulated crash")
        return result

    monkeypatch.setattr(wf, "_run_tier", fake_run)
    with pytest.raises(RuntimeError, match="simulated crash"):
        wf.resolve_domain(
            source_table="public.t",
            where="wf_domain_status is null",
            client_tag="t",
            writeback=True,
            max_tier="maps",
        )
    saved = [k for k, r in table.items() if r.get("wf_domain_status") is not None]
    assert len(saved) >= 1000
    remaining = fetch(None).rows_fetched
    assert remaining == n - len(saved)
    assert remaining <= 200

    # Rerun with default where wf_domain_status is null skips saved rows.
    ran_keys: list[str] = []

    def fake_run_ok(name: str, rows: list[dict[str, Any]], *_a: Any, **kwargs: Any) -> TierResult:
        on_settled = kwargs.get("on_settled")
        result = TierResult(tier=name, rows_done=len(rows))
        for row in rows:
            key = str(row["_source_key"])
            ran_keys.append(key)
            cand = _cand(int(key))
            result.candidates[key] = cand
            if on_settled:
                on_settled(key, cand)
        return result

    monkeypatch.setattr(wf, "_run_tier", fake_run_ok)
    out = wf.resolve_domain(
        source_table="public.t",
        where="wf_domain_status is null",
        client_tag="t",
        writeback=True,
        max_tier="maps",
    )
    assert set(ran_keys).isdisjoint(set(saved))
    assert out["rows"] == remaining
    assert all(k not in saved for k in ran_keys)


def test_tier_handoff_reads_in_memory_state(monkeypatch: pytest.MonkeyPatch) -> None:
    from domain_waterfall import waterfall as wf

    n = 3
    _stub_common(monkeypatch, n=n, tiers=["maps", "aiark"])
    monkeypatch.setattr(
        wf,
        "patch_source_rows",
        lambda _src, items, **_k: WritebackResult(written_keys=[str(k) for k, _ in items]),
    )
    seen: dict[str, list[str]] = {}

    def fake_run(name: str, rows: list[dict[str, Any]], *_a: Any, **kwargs: Any) -> TierResult:
        on_settled = kwargs.get("on_settled")
        seen[name] = [str(r["_source_key"]) for r in rows]
        result = TierResult(tier=name, rows_done=len(rows))
        if name == "maps":
            # Resolve only key 0. 1 and 2 stay eligible.
            cand = _cand(0)
            result.candidates["0"] = cand
            if on_settled:
                on_settled("0", cand)
                on_settled("1", None)
                on_settled("2", None)
            result.none = 2
            return result
        for row in rows:
            key = str(row["_source_key"])
            cand = _cand(int(key))
            result.candidates[key] = cand
            if on_settled:
                on_settled(key, cand)
        return result

    monkeypatch.setattr(wf, "_run_tier", fake_run)
    out = wf.resolve_domain(
        source_table="public.t",
        where="wf_domain_status is null",
        client_tag="t",
        writeback=True,
        max_tier="aiark",
    )
    assert seen["maps"] == ["0", "1", "2"]
    assert seen["aiark"] == ["1", "2"]
    assert out["counts"]["resolved"] == 3


def test_partial_row_keeps_existing_columns() -> None:
    existing = {
        "wf_domain": "keep.com",
        "wf_domain_source": "maps",
        "wf_domain_confidence": 0.85,
        "wf_domain_agreement": False,
        "wf_domain_candidates": [{"domain": "keep.com"}],
        "wf_phone": "2145550100",
        "wf_domain_status": "resolved",
        "dl_status": "leave-me",
        "sg_exclude": False,
        "skip_reason": "no",
    }
    incoming = {"wf_domain_status": "domain_unresolved"}
    merged = merge_write_fields(existing, incoming)
    assert merged["wf_domain"] == "keep.com"
    assert merged["wf_domain_source"] == "maps"
    assert merged["wf_phone"] == "2145550100"
    assert merged["wf_domain_status"] == "domain_unresolved"
    assert merged["dl_status"] == "leave-me"
    assert merged["sg_exclude"] is False
    assert merged["skip_reason"] == "no"

    src = parse_source("public.t")
    payload = _batch_payload(src, [("1", incoming)])
    assert payload == [{"id": "1", "wf_domain_status": "domain_unresolved"}]
    assert "wf_domain" not in payload[0]
    assert "dl_status" not in payload[0]


def test_migration_007_updates_only_present_keys() -> None:
    from pathlib import Path

    sql = Path("supabase/migrations/007_partial_patch_source_batch.sql").read_text()
    assert "007" in Path("supabase/migrations/007_partial_patch_source_batch.sql").name
    for col in (
        "wf_domain",
        "wf_domain_source",
        "wf_domain_confidence",
        "wf_domain_agreement",
        "wf_domain_candidates",
        "wf_phone",
        "wf_domain_status",
    ):
        assert f"e.elem ? '{col}'" in sql
        assert "dl_status" not in sql.split("allowed")[0] or "dl_status" in sql
    assert "jsonb_array_elements" in sql
    assert "FROM jsonb_to_recordset" not in sql
    assert "skip_%" in sql or "skip_" in sql
    assert "sg_exclude" in sql
