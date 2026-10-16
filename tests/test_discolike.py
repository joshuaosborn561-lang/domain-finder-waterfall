import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from domain_waterfall.pricing import PUBLISHED, compute_order, estimate_rows
from domain_waterfall.profiles import ClientProfile
from domain_waterfall.vendors import discolike
from domain_waterfall.vendors.discolike import UNIT


def _resp(status: int, payload: object) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.json.return_value = payload
    return r


def _row(i: int, *, state: str = "FL") -> dict[str, Any]:
    return {
        "_source_key": str(i),
        "company_name": f"Acme{i:04d}",
        "city": "Tampa",
        "state": state,
        "phone": "",
    }


def _hit(key: str, *, state: str = "Florida", error: str | None = None) -> dict[str, Any]:
    item = {
        "input:row_key": key,
        "domain": f"acme{key}.com",
        "name": f"Acme{key}",
        "address": {"state": state, "city": "Tampa"},
    }
    if error:
        item["match_error"] = error
        item["domain"] = ""
    return item


@pytest.fixture(autouse=True)
def _key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(discolike.settings, "discolike_api_key", "fake")


def test_profile_defaults_relax_vendor_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, str]] = []

    def fake_post(_tier: str, _url: str, **kwargs: Any) -> MagicMock:
        seen.append(dict(kwargs.get("params") or {}))
        return _resp(200, {"task_id": "t1"})

    monkeypatch.setattr(discolike.http_client, "post", fake_post)
    monkeypatch.setattr(
        discolike.http_client,
        "get",
        lambda *_a, **_k: _resp(200, {"status": "completed", "results": [], "billed": 0}),
    )
    profile = ClientProfile(client_tag="t", raw={})
    out = discolike.resolve_rows([_row(1)], profile=profile, sleep=lambda _s: None)
    assert seen[0]["min_match_confidence"] == "60"
    assert seen[0]["strict"] == "false"
    assert profile.discolike_min_confidence == 60
    assert profile.discolike_strict is False
    assert out.error is None


def test_profile_overrides_confidence_and_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, str]] = []

    def fake_post(_tier: str, _url: str, **kwargs: Any) -> MagicMock:
        seen.append(dict(kwargs.get("params") or {}))
        return _resp(200, {"task_id": "t1"})

    monkeypatch.setattr(discolike.http_client, "post", fake_post)
    monkeypatch.setattr(
        discolike.http_client,
        "get",
        lambda *_a, **_k: _resp(200, {"status": "completed", "results": [], "billed": 0}),
    )
    profile = ClientProfile(
        client_tag="t",
        raw={"discolike_min_confidence": 70, "discolike_strict": True},
    )
    discolike.resolve_rows([_row(1)], profile=profile, sleep=lambda _s: None)
    assert seen[0]["min_match_confidence"] == "70"
    assert seen[0]["strict"] == "true"
    assert profile.discolike_min_confidence == 70
    assert profile.discolike_strict is True


def test_chunks_run_sequentially_never_concurrent(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    in_flight = {"n": 0, "max": 0}
    task_n = {"n": 0}

    def fake_post(_tier: str, _url: str, **_k: Any) -> MagicMock:
        in_flight["n"] += 1
        in_flight["max"] = max(in_flight["max"], in_flight["n"])
        task_n["n"] += 1
        events.append(f"post:{task_n['n']}")
        return _resp(200, {"task_id": f"t{task_n['n']}"})

    def fake_get(_tier: str, url: str, **_k: Any) -> MagicMock:
        events.append(f"poll:{url.rsplit('/', 1)[-1]}")
        in_flight["n"] = 0
        task_id = url.rsplit("/", 1)[-1]
        key = task_id.replace("t", "")
        return _resp(
            200,
            {
                "status": "completed",
                "results": [_hit(str(int(key) - 1))],
                "billed": 1,
            },
        )

    monkeypatch.setattr(discolike.http_client, "post", fake_post)
    monkeypatch.setattr(discolike.http_client, "get", fake_get)
    rows = [_row(i) for i in range(5)]

    def on_settled(key: str, cand: Any, **_k: Any) -> None:
        events.append(f"settled:{key}")

    out = discolike.resolve_rows(
        rows,
        chunk_size=2,
        on_settled=on_settled,
        sleep=lambda _s: None,
    )
    settled = [e for e in events if e.startswith("settled:")]
    assert settled[:2] == ["settled:0", "settled:1"]
    assert in_flight["max"] == 1
    posts = [e for e in events if e.startswith("post:")]
    assert posts == ["post:1", "post:2", "post:3"]
    # Each post is followed by its poll, then on_settled, before the next post.
    assert events[:2] == ["post:1", "poll:t1"]
    assert events.index("settled:0") < events.index("post:2")
    assert events.index("settled:1") < events.index("post:2")
    assert events.index("poll:t2") < events.index("post:3")
    assert out.error is None
    assert len(out.candidates) == 3


def test_chunk_size_caps_at_1000() -> None:
    assert discolike.resolve_chunk_size(5000) == 1000
    assert discolike.resolve_chunk_size(500) == 500
    assert discolike.resolve_chunk_size(1) == 1
    profile = ClientProfile(client_tag="t", raw={"discolike_chunk_size": 800})
    assert discolike.resolve_chunk_size(profile=profile) == 800


def test_timeout_on_settled_is_deferred_not_miss(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"t": 0.0}
    calls: list[tuple[str, Any, bool]] = []

    monkeypatch.setattr(
        discolike.http_client,
        "post",
        lambda *_a, **_k: _resp(200, {"task_id": "slow"}),
    )
    monkeypatch.setattr(
        discolike.http_client,
        "get",
        lambda *_a, **_k: _resp(200, {"status": "running"}),
    )

    def on_settled(key: str, cand: Any, deferred: bool = False, **_k: Any) -> None:
        calls.append((key, cand, deferred))

    out = discolike.resolve_rows(
        [_row(1), _row(2)],
        poll_timeout_s=10,
        on_settled=on_settled,
        sleep=lambda s: clock.__setitem__("t", clock["t"] + s),
        clock=lambda: clock["t"],
    )
    assert out.error == "bulkmatch_timeout"
    assert sorted(k for k, _c, _d in calls) == ["1", "2"]
    assert all(cand is None for _k, cand, _d in calls)
    assert all(deferred is True for _k, _c, deferred in calls)


def test_timeout_marks_deferred_not_silent_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"t": 0.0}

    def fake_clock() -> float:
        return clock["t"]

    def fake_sleep(seconds: float) -> None:
        clock["t"] += float(seconds)

    monkeypatch.setattr(
        discolike.http_client,
        "post",
        lambda *_a, **_k: _resp(200, {"task_id": "slow"}),
    )
    monkeypatch.setattr(
        discolike.http_client,
        "get",
        lambda *_a, **_k: _resp(200, {"status": "running"}),
    )
    rows = [_row(1), _row(2)]
    out = discolike.resolve_rows(
        rows,
        poll_timeout_s=10,
        sleep=fake_sleep,
        clock=fake_clock,
    )
    assert out.error == "bulkmatch_timeout"
    assert sorted(out.deferred_keys) == ["1", "2"]
    assert out.candidates == {}
    assert out.none == 0
    assert out.errored >= 2
    assert clock["t"] >= 10


def test_failed_status_defers_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        discolike.http_client,
        "post",
        lambda *_a, **_k: _resp(200, {"task_id": "x"}),
    )
    monkeypatch.setattr(
        discolike.http_client,
        "get",
        lambda *_a, **_k: _resp(200, {"status": "failed"}),
    )
    out = discolike.resolve_rows([_row(9)], sleep=lambda _s: None)
    assert out.error == "bulkmatch_failed"
    assert out.deferred_keys == ["9"]
    assert out.none == 0


def test_second_chunk_timeout_keeps_first_and_defers_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posts = {"n": 0}
    clock = {"t": 0.0}

    def fake_post(*_a: Any, **_k: Any) -> MagicMock:
        posts["n"] += 1
        return _resp(200, {"task_id": f"t{posts['n']}"})

    def fake_get(_tier: str, url: str, **_k: Any) -> MagicMock:
        task = url.rsplit("/", 1)[-1]
        if task == "t1":
            return _resp(
                200,
                {"status": "completed", "results": [_hit("0")], "billed": 1},
            )
        return _resp(200, {"status": "running"})

    monkeypatch.setattr(discolike.http_client, "post", fake_post)
    monkeypatch.setattr(discolike.http_client, "get", fake_get)
    rows = [_row(i) for i in range(3)]
    out = discolike.resolve_rows(
        rows,
        chunk_size=2,
        poll_timeout_s=8,
        sleep=lambda s: clock.__setitem__("t", clock["t"] + s),
        clock=lambda: clock["t"],
    )
    assert "0" in out.candidates
    assert out.error == "bulkmatch_timeout"
    assert sorted(out.deferred_keys) == ["2"]
    assert out.cost_usd == pytest.approx(UNIT)
    assert out.billed_calls == 1


def test_cost_uses_status_billed_count(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        discolike.http_client,
        "post",
        lambda *_a, **_k: _resp(200, {"task_id": "t1"}),
    )
    monkeypatch.setattr(
        discolike.http_client,
        "get",
        lambda *_a, **_k: _resp(
            200,
            {
                "status": "completed",
                "billed": 2,
                "results": [_hit("1"), _hit("2"), _hit("3", error="low")],
            },
        ),
    )
    out = discolike.resolve_rows([_row(1), _row(2), _row(3)], sleep=lambda _s: None)
    assert out.billed_calls == 2
    assert out.cost_usd == pytest.approx(2 * UNIT)
    assert out.cost_usd != pytest.approx(3 * UNIT)


def test_cost_falls_back_to_results_without_match_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        discolike.http_client,
        "post",
        lambda *_a, **_k: _resp(200, {"task_id": "t1"}),
    )
    monkeypatch.setattr(
        discolike.http_client,
        "get",
        lambda *_a, **_k: _resp(
            200,
            {
                "status": "completed",
                "results": [_hit("1"), _hit("2", error="none"), _hit("3")],
            },
        ),
    )
    out = discolike.resolve_rows([_row(1), _row(2), _row(3)], sleep=lambda _s: None)
    assert out.billed_calls == 2
    assert out.cost_usd == pytest.approx(2 * UNIT)
    assert "1" in out.candidates
    assert "3" in out.candidates
    assert "2" not in out.candidates


def test_estimate_only_keeps_unit_times_rows_ceiling() -> None:
    order = compute_order(["discolike"], live_units={"discolike": UNIT})
    estimate = estimate_rows(100, order)
    assert estimate["estimated_usd"] == round(100 * UNIT, 4)
    assert estimate["tiers"][0]["estimated_usd"] == round(100 * UNIT, 6)
    assert PUBLISHED["discolike"]["unit"] == UNIT


def test_poll_backoff_caps_at_15s(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    clock = {"t": 0.0}

    monkeypatch.setattr(
        discolike.http_client,
        "post",
        lambda *_a, **_k: _resp(200, {"task_id": "t1"}),
    )
    monkeypatch.setattr(
        discolike.http_client,
        "get",
        lambda *_a, **_k: _resp(200, {"status": "running"}),
    )
    discolike.resolve_rows(
        [_row(1)],
        poll_timeout_s=40,
        sleep=lambda s: (sleeps.append(s), clock.__setitem__("t", clock["t"] + s)),
        clock=lambda: clock["t"],
    )
    assert sleeps[0] == 2.0
    assert max(sleeps) == 15.0
    assert sleeps == sorted(sleeps) or sleeps[-1] == 15.0


def test_waterfall_timeout_writes_deferred_not_unresolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from domain_waterfall import waterfall as wf
    from domain_waterfall.source import FetchResult, WritebackResult
    from domain_waterfall.vendors.base import TierResult

    profile = ClientProfile(
        client_tag="t",
        raw={
            "enabled_tiers": ["discolike"],
            "geo": {"geo_required": False},
            "discolike_chunk_size": 2,
        },
    )
    monkeypatch.setattr(wf, "get_profile", lambda _tag: profile)
    monkeypatch.setattr(wf, "hydrate_keys", lambda: {"discolike": True})
    monkeypatch.setattr(
        wf,
        "live_prices",
        lambda: ({"discolike": UNIT, "cache": 0.0, "maps": 0.0}, {}),
    )
    fetched = FetchResult(
        rows=[_row(1), _row(2)],
        rows_matched=2,
        rows_fetched=2,
        rows_excluded=0,
        exclusion_reasons={},
    )
    monkeypatch.setattr(wf, "fetch_source_rows", lambda _src: fetched)
    monkeypatch.setattr(wf, "count_source_rows", lambda _src: 2)
    monkeypatch.setattr(wf, "ensure_writeback", lambda _src: None)
    monkeypatch.setattr(wf, "defer_unfetched", lambda *_a, **_k: 0)
    monkeypatch.setattr(wf, "preflight_write_columns", lambda *_a, **_k: None)
    patches: list[dict[str, Any]] = []

    def capture(_src: Any, items: list, **_k: Any) -> WritebackResult:
        keys = []
        for key, fields in items:
            patches.append(dict(fields))
            keys.append(str(key))
        return WritebackResult(written_keys=keys)

    monkeypatch.setattr(wf, "patch_source_rows", capture)
    monkeypatch.setattr(
        wf,
        "_run_tier",
        lambda name, rows, *_a, **_k: TierResult(
            tier=name,
            error="bulkmatch_timeout",
            deferred_keys=[str(r["_source_key"]) for r in rows],
            rows_done=0,
        ),
    )
    out = wf.resolve_domain(
        source_table="public.t",
        where="domain is null",
        client_tag="t",
        writeback=True,
        max_tier="discolike",
    )
    assert out["counts"]["deferred"] == 2
    assert out["counts"]["domain_unresolved"] == 0
    assert any(p.get("wf_domain_status") == "deferred" for p in patches)
    assert out["tiers"][0]["error"] == "bulkmatch_timeout"


def test_waterfall_saves_after_each_chunk(monkeypatch: pytest.MonkeyPatch) -> None:
    from domain_waterfall import waterfall as wf
    from domain_waterfall.source import FetchResult, WritebackResult
    from domain_waterfall.vendors.base import DomainCandidate, TierResult

    profile = ClientProfile(
        client_tag="t",
        raw={
            "enabled_tiers": ["discolike"],
            "geo": {"geo_required": False},
            "name_strip_tokens": ["inc", "llc"],
            "discolike_chunk_size": 2,
        },
    )
    monkeypatch.setattr(wf, "get_profile", lambda _tag: profile)
    monkeypatch.setattr(wf, "hydrate_keys", lambda: {"discolike": True})
    monkeypatch.setattr(
        wf,
        "live_prices",
        lambda: ({"discolike": UNIT, "cache": 0.0, "maps": 0.0}, {}),
    )
    rows = [_row(i) for i in range(3)]
    fetched = FetchResult(
        rows=rows,
        rows_matched=3,
        rows_fetched=3,
        rows_excluded=0,
        exclusion_reasons={},
    )
    monkeypatch.setattr(wf, "fetch_source_rows", lambda _src: fetched)
    monkeypatch.setattr(wf, "count_source_rows", lambda _src: 3)
    monkeypatch.setattr(wf, "ensure_writeback", lambda _src: None)
    monkeypatch.setattr(wf, "defer_unfetched", lambda *_a, **_k: 0)
    monkeypatch.setattr(wf, "preflight_write_columns", lambda *_a, **_k: None)
    saved_before_second: list[int] = []
    patches: list[str] = []

    def capture(_src: Any, items: list, **_k: Any) -> WritebackResult:
        keys = [str(k) for k, _ in items]
        patches.extend(keys)
        return WritebackResult(written_keys=keys)

    monkeypatch.setattr(wf, "patch_source_rows", capture)

    monkeypatch.setenv("WRITEBACK_FLUSH_INTERVAL_S", "0.05")

    def fake_run(name: str, batch: list[dict[str, Any]], *_a: Any, **kwargs: Any) -> TierResult:
        on_settled = kwargs.get("on_settled")
        result = TierResult(tier=name)
        first = batch[:2]
        for row in first:
            key = str(row["_source_key"])
            cand = DomainCandidate(
                domain=f"acme{int(key):04d}.com",
                vendor_name=f"Acme{int(key):04d}",
            )
            result.candidates[key] = cand
            if on_settled:
                on_settled(key, cand)
        result.cost_usd += UNIT * 2
        result.billed_calls += 2
        deadline = time.time() + 3
        while time.time() < deadline:
            if len(patches) >= 2:
                break
            time.sleep(0.02)
        saved_before_second.append(len(patches))
        last = batch[2:]
        result.error = "bulkmatch_timeout"
        for row in last:
            key = str(row["_source_key"])
            result.deferred_keys.append(key)
            if on_settled:
                on_settled(key, None, deferred=True)
        return result

    monkeypatch.setattr(wf, "_run_tier", fake_run)
    out = wf.resolve_domain(
        source_table="public.t",
        where="domain is null",
        client_tag="t",
        writeback=True,
        max_tier="discolike",
    )
    assert saved_before_second[0] >= 1
    assert out["counts"]["deferred"] == 1
    assert out["counts"]["domain_unresolved"] == 0
    assert out["counts"]["resolved"] == 2
    assert out["spent_usd"] == round(2 * UNIT, 4)
