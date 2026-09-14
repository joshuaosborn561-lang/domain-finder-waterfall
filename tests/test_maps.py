from unittest.mock import MagicMock

import pytest

from domain_waterfall.concurrency import VendorCallTimeout
from domain_waterfall.vendors import maps


def test_maps_not_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "")
    result = maps.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin"}]
    )
    assert result.skipped == "not configured"
    assert result.candidates == {}


def test_maps_timeout_is_miss_and_continues(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "fake")
    monkeypatch.setattr(maps.settings, "maps_host", "maps-data.p.rapidapi.com")
    calls = {"n": 0}

    def boom(*_a: object, **_k: object) -> None:
        calls["n"] += 1
        raise VendorCallTimeout("maps")

    monkeypatch.setattr(maps.http_client, "get", boom)
    rows = [
        {"_source_key": "1", "company_name": "Acme", "city": "Austin"},
        {"_source_key": "2", "company_name": "Beta", "city": "Dallas"},
    ]
    result = maps.resolve_rows(rows)
    assert result.none == 2
    assert result.candidates == {}
    assert calls["n"] == 2
    assert result.skipped is None
    assert result.error is None


def test_maps_three_timeouts_skip_rest(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "fake")
    monkeypatch.setattr(maps.settings, "maps_host", "maps-data.p.rapidapi.com")
    calls = {"n": 0}

    def boom(*_a: object, **_k: object) -> None:
        calls["n"] += 1
        raise VendorCallTimeout("maps")

    monkeypatch.setattr(maps.http_client, "get", boom)
    rows = [
        {"_source_key": str(i), "company_name": f"Co{i}", "city": "Austin"}
        for i in range(6)
    ]
    result = maps.resolve_rows(rows)
    assert result.skipped == "vendor unresponsive"
    assert result.error == "tier timeout"
    assert calls["n"] == 3
    assert result.none == 6


def test_maps_deadline_stops_and_records_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "fake")
    monkeypatch.setattr(maps.settings, "maps_host", "maps-data.p.rapidapi.com")

    def fake_get(*_a: object, **_k: object) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = []
        return resp

    monkeypatch.setattr(maps.http_client, "get", fake_get)
    rows = [
        {"_source_key": str(i), "company_name": f"Co{i}", "city": "Austin"}
        for i in range(5)
    ]
    result = maps.resolve_rows(rows, deadline=0.0)
    assert result.error == "tier timeout"
    assert result.none == 5
    assert result.candidates == {}


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
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin"}]
    )
    assert "1" in result.candidates
    assert result.candidates["1"].domain == "acme.com"
    assert all("place.php" not in u for u in urls)


def test_maps_nested_website(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(maps.settings, "rapidapi_key", "fake")
    monkeypatch.setattr(maps.settings, "maps_host", "maps-data.p.rapidapi.com")

    def fake_get(_tier: str, url: str, **_k: object) -> MagicMock:
        resp = MagicMock()
        resp.status_code = 200
        if "searchmaps" in url:
            resp.json.return_value = [
                {"name": "Acme", "website": {"url": "https://nested.example"}, "place_id": "Chxx"}
            ]
        else:
            raise AssertionError("details should be skipped")
        return resp

    monkeypatch.setattr(maps.http_client, "get", fake_get)
    result = maps.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin"}]
    )
    assert result.candidates["1"].domain == "nested.example"
