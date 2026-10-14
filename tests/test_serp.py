from unittest.mock import MagicMock

import pytest

from domain_waterfall.concurrency import VendorThrottle
from domain_waterfall.vendors import serp


def _resp(status: int, payload: object, text: str = "") -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = text
    r.json.return_value = payload
    return r


def _row(key: str, name: str, city: str = "Austin", state: str = "TX") -> dict[str, str]:
    return {"_source_key": key, "company_name": name, "city": city, "state": state}


def test_query_is_name_city_state() -> None:
    assert serp.build_serp_query("Acme", "Austin", "TX") == "Acme Austin TX"
    assert serp.build_serp_query("Acme", "", "") == "Acme"
    assert serp.build_serp_query("Acme", "Austin", "") == "Acme Austin"


def test_organic_skips_sinks_and_requires_name_tokens() -> None:
    item = {
        "organic": [
            {"link": "https://waze.com/place/1", "title": "Acme Plumbing, Austin"},
            {"link": "https://foo.cybo.com/acme", "title": "Acme Plumbing"},
            {"link": "https://machinist.com/shops/acme", "title": "Acme Plumbing"},
            {"link": "https://eacsociety.org/members", "title": "Acme Plumbing"},
            {"link": "https://acmeplumbing.com", "title": "Acme Plumbing"},
        ]
    }
    domain, _title = serp._domain_from_organic(item, company_name="Acme Plumbing")
    assert domain == "acmeplumbing.com"


def test_resolve_rows_maps_by_index_skips_blocklist_and_bills(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    posted: list[object] = []

    def fake_post(_tier: str, url: str, **kwargs: object) -> MagicMock:
        posted.append({"url": url, "json": kwargs.get("json"), "headers": kwargs.get("headers")})
        return _resp(
            200,
            [
                {
                    # q is swapped on purpose: index mapping must win.
                    "searchParameters": {"q": "Beta Dallas TX"},
                    "organic": [
                        {"link": "https://waze.com/place/1", "title": "Acme Plumbing"},
                        {"link": "https://acmeplumbing.com", "title": "Acme Plumbing"},
                    ],
                },
                {
                    "searchParameters": {"q": "Acme Austin TX"},
                    "organic": [
                        {"link": "https://linkedin.com/company/beta", "title": "Beta Pipes"},
                        {"link": "https://betapipes.com", "title": "Beta Pipes"},
                    ],
                },
            ],
        )

    monkeypatch.setattr(serp.settings, "serper_api_key", "serper-test")
    monkeypatch.setattr(serp.http_client, "post", fake_post)
    out = serp.resolve_rows(
        [
            _row("1", "Acme Plumbing", "Austin", "TX"),
            _row("2", "Beta Pipes", "Dallas", "TX"),
        ],
        unit=0.001,
        concurrency=1,
    )
    assert posted and posted[0]["url"] == "https://google.serper.dev/search"
    assert posted[0]["headers"]["X-API-KEY"] == "serper-test"
    assert posted[0]["json"] == [
        {"q": "Acme Plumbing Austin TX", "gl": "us", "hl": "en", "num": 10},
        {"q": "Beta Pipes Dallas TX", "gl": "us", "hl": "en", "num": 10},
    ]
    assert "apify" not in posted[0]["url"]
    assert out.candidates["1"].domain == "acmeplumbing.com"
    assert out.candidates["2"].domain == "betapipes.com"
    assert out.cost_usd == pytest.approx(0.002)
    assert out.billed_calls == 2
    assert out.billing == "serper google search"
    assert out.inputs_passed == ["company_name", "city", "state"]
    assert out.errored == 0
    assert out.skipped is None


def test_pack_falls_back_to_search_parameters_q() -> None:
    queries = [
        {"key": "1", "q": "Acme Austin TX", "name": "Acme"},
        {"key": "2", "q": "Beta Dallas TX", "name": "Beta"},
    ]
    items = [
        "not-a-result",
        {
            "searchParameters": {"q": "Acme Austin TX"},
            "organic": [{"link": "https://acme.com", "title": "Acme"}],
        },
    ]
    packed = serp._pack_items(queries, items, unit=0.001)
    by_key = {p["key"]: p for p in packed}
    assert by_key["1"]["domain"] == "acme.com"
    assert by_key["2"].get("none") is True


def test_token_match_required_in_resolve_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(_tier: str, url: str, **_k: object) -> MagicMock:
        return _resp(
            200,
            [
                {
                    "searchParameters": {"q": "Acme Plumbing Austin TX"},
                    "organic": [
                        {"link": "https://unrelated-widgets.com", "title": "Acme Plumbing"},
                        {"link": "https://acmeplumbing.com", "title": "Acme Plumbing"},
                    ],
                }
            ],
        )

    monkeypatch.setattr(serp.settings, "serper_api_key", "k")
    monkeypatch.setattr(serp.http_client, "post", fake_post)
    out = serp.resolve_rows([_row("1", "Acme Plumbing")], concurrency=1)
    assert out.candidates["1"].domain == "acmeplumbing.com"


def test_missing_key_skips_and_never_calls_apify(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[str] = []

    def fake_post(_tier: str, url: str, **_k: object) -> MagicMock:
        posted.append(url)
        raise AssertionError("serp must not POST without SERPER_API_KEY")

    monkeypatch.setattr(serp.settings, "serper_api_key", "")
    if hasattr(serp.settings, "apify_token"):
        monkeypatch.setattr(serp.settings, "apify_token", "tok")
    monkeypatch.setattr(serp.http_client, "post", fake_post)
    out = serp.resolve_rows([_row("1", "Acme")], concurrency=1)
    assert out.skipped == "serper_key_missing"
    assert out.candidates == {}
    assert out.cost_usd == 0.0
    assert posted == []


def test_429_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp, "_retry_backoff", lambda _attempt: None)
    calls = {"n": 0}

    def fake_post(_tier: str, url: str, **_k: object) -> MagicMock:
        calls["n"] += 1
        if calls["n"] == 1:
            return _resp(429, {"message": "rate limited"})
        return _resp(
            200,
            [
                {
                    "searchParameters": {"q": "Acme Austin TX"},
                    "organic": [{"link": "https://acme.com", "title": "Acme"}],
                }
            ],
        )

    monkeypatch.setattr(serp.settings, "serper_api_key", "k")
    monkeypatch.setattr(serp.http_client, "post", fake_post)
    out = serp.resolve_rows([_row("1", "Acme")], unit=0.001, concurrency=1)
    assert calls["n"] == 2
    assert "1" in out.candidates
    assert out.candidates["1"].domain == "acme.com"
    assert out.cost_usd == pytest.approx(0.001)


def test_429_exhausted_marks_chunk_errored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp, "_retry_backoff", lambda _attempt: None)
    import domain_waterfall.tier_pool as pool

    monkeypatch.setattr(pool, "ROW_ERROR_RETRIES", 1)

    def fake_post(_tier: str, url: str, **_k: object) -> MagicMock:
        return _resp(429, {"message": "rate limited"})

    monkeypatch.setattr(serp.settings, "serper_api_key", "k")
    monkeypatch.setattr(serp.http_client, "post", fake_post)
    out = serp.resolve_rows([_row("1", "Acme")], concurrency=1)
    assert out.candidates == {}
    assert out.cost_usd == 0.0
    assert out.errored >= 1
    assert out.error
    assert "429" in (out.error or "")


def test_live_unit_price_default_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp.settings, "serper_api_key", "k")
    monkeypatch.delenv("SERPER_UNIT_USD", raising=False)
    unit, info = serp.live_unit_price()
    assert unit == 0.001
    assert info["billing"] == "serper google search"
    assert info["source"] == "default"
    monkeypatch.setenv("SERPER_UNIT_USD", "0.0025")
    unit, info = serp.live_unit_price()
    assert unit == 0.0025
    assert info["source"] == "env"


def test_transport_error_does_not_bill(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp, "_retry_backoff", lambda _attempt: None)
    import domain_waterfall.tier_pool as pool

    monkeypatch.setattr(pool, "ROW_ERROR_RETRIES", 1)

    def fake_post(_tier: str, url: str, **_k: object) -> MagicMock:
        return _resp(503, {"message": "unavailable"})

    monkeypatch.setattr(serp.settings, "serper_api_key", "k")
    monkeypatch.setattr(serp.http_client, "post", fake_post)
    rows = [_row(str(i), f"Co{i}") for i in range(150)]
    out = serp.resolve_rows(rows, concurrency=1)
    assert out.cost_usd == 0.0
    assert out.billed_calls == 0
    assert out.errored >= 150
    assert not out.candidates


def test_429_raises_throttle_after_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp, "_retry_backoff", lambda _attempt: None)
    monkeypatch.setattr(serp.settings, "serper_api_key", "k")
    monkeypatch.setattr(serp.http_client, "post", lambda *_a, **_k: _resp(429, {}))
    with pytest.raises(VendorThrottle):
        serp._search_chunk([{"q": "Acme Austin TX"}])
