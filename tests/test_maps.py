from unittest.mock import MagicMock

import pytest

from domain_waterfall.concurrency import VendorCallTimeout, VendorThrottle, VendorTransportError
from domain_waterfall.tier_pool import resolve_tier_concurrency
from domain_waterfall.vendors import maps


def test_resolve_tier_concurrency_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TIER_CONCURRENCY", raising=False)
    monkeypatch.delenv("MAPS_TIER_CONCURRENCY", raising=False)
    assert resolve_tier_concurrency("maps") == 12
    assert resolve_tier_concurrency("maps", 100) == 32
    assert resolve_tier_concurrency("maps", 1) == 1
    monkeypatch.setenv("MAPS_TIER_CONCURRENCY", "8")
    assert resolve_tier_concurrency("maps") == 8


def test_maps_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "")
    result = maps.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin"}]
    )
    assert result.skipped == "not configured"
    assert result.candidates == {}
    assert result.billing.startswith("free")
    assert result.cost_usd == 0.0


def test_maps_timeout_retries_then_errored_not_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "fake")
    monkeypatch.setattr(maps.settings, "maps_host", "maps-data.p.rapidapi.com")
    calls = {"n": 0}

    def boom(*_a: object, **_k: object) -> None:
        calls["n"] += 1
        raise VendorCallTimeout("maps")

    monkeypatch.setattr(maps.http_client, "get", boom)
    rows = [{"_source_key": "1", "company_name": "Acme", "city": "Austin"}]
    result = maps.resolve_rows(rows, concurrency=1)
    assert result.errored == 1
    assert result.none == 0
    assert result.candidates == {}
    assert calls["n"] >= 3


def test_maps_true_miss_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "fake")
    monkeypatch.setattr(maps.settings, "maps_host", "maps-data.p.rapidapi.com")

    def empty(*_a: object, **_k: object) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = []
        return resp

    monkeypatch.setattr(maps.http_client, "get", empty)
    result = maps.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin"}],
        concurrency=1,
    )
    assert result.none == 1
    assert result.errored == 0


def test_maps_429_retries_not_miss(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "fake")
    monkeypatch.setattr(maps.settings, "maps_host", "maps-data.p.rapidapi.com")
    n = {"i": 0}

    def flaky(*_a: object, **_k: object) -> MagicMock:
        n["i"] += 1
        if n["i"] < 3:
            raise VendorThrottle("maps", "http 429")
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = [
            {"name": "Acme", "website": "https://acme.com", "place_id": "Chxx"}
        ]
        return resp

    monkeypatch.setattr(maps.http_client, "get", flaky)
    # Speed up retries
    import domain_waterfall.tier_pool as pool

    monkeypatch.setattr(pool, "_retry_sleep", lambda _a: None)
    result = maps.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin"}],
        concurrency=1,
    )
    assert "1" in result.candidates
    assert result.errored == 0
    assert result.none == 0


def test_maps_parallel_preserves_accepts(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "fake")
    monkeypatch.setattr(maps.settings, "maps_host", "maps-data.p.rapidapi.com")

    def ok(_tier: str, url: str, **_k: object) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        if "searchmaps" in url:
            resp.json.return_value = [
                {"name": "Acme", "website": {"url": "https://acme.example"}, "place_id": "Chxx"}
            ]
        else:
            raise AssertionError("details should be skipped")
        return resp

    monkeypatch.setattr(maps.http_client, "get", ok)
    rows = [
        {"_source_key": str(i), "company_name": f"Co{i}", "city": "Austin"} for i in range(20)
    ]
    result = maps.resolve_rows(rows, concurrency=8)
    assert len(result.candidates) == 20
    assert result.errored == 0
    assert result.none == 0
    assert result.cost_usd == 0.0
    assert "free" in result.billing


def test_maps_skips_details_when_search_has_website(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "fake")
    monkeypatch.setattr(maps.settings, "maps_host", "maps-data.p.rapidapi.com")
    urls: list[str] = []

    def fake_get(_tier: str, url: str, **_k: object) -> MagicMock:
        urls.append(url)
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = [
            {"name": "Acme", "website": "https://acme.com", "place_id": "Chxx"}
        ]
        return resp

    monkeypatch.setattr(maps.http_client, "get", fake_get)
    result = maps.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin"}],
        concurrency=1,
    )
    assert "1" in result.candidates
    assert result.candidates["1"].domain == "acme.com"
    assert all("place.php" not in u for u in urls)


def test_maps_deadline_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "fake")
    monkeypatch.setattr(maps.settings, "maps_host", "maps-data.p.rapidapi.com")

    def fake_get(*_a: object, **_k: object) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = []
        return resp

    monkeypatch.setattr(maps.http_client, "get", fake_get)
    rows = [
        {"_source_key": str(i), "company_name": f"Co{i}", "city": "Austin"} for i in range(5)
    ]
    result = maps.resolve_rows(rows, deadline=0.0, concurrency=1)
    assert result.error == "tier timeout"
