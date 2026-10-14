from unittest.mock import MagicMock

import pytest

from domain_waterfall.vendors import serp


def _resp(status: int, payload: object, text: str = "") -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = text
    r.json.return_value = payload
    return r


def _serper_item(q: str, link: str, title: str) -> dict:
    return {
        "searchParameters": {"q": q},
        "organic": [{"title": title, "link": link, "snippet": "", "position": 1}],
    }


def test_query_is_name_city_state() -> None:
    assert serp.build_serp_query("Acme", "Austin", "TX") == "Acme Austin TX"
    assert serp.build_serp_query("Acme", "", "") == "Acme"
    assert serp.build_serp_query("Acme", "Austin", "") == "Acme Austin"


def test_organic_skips_blocklisted_and_requires_name_tokens() -> None:
    item = {
        "organic": [
            {"link": "https://waze.com/place/1", "title": "Acme Plumbing, Austin"},
            {"link": "https://foo.cybo.com/acme", "title": "Acme Plumbing"},
            {"link": "https://machinist.com/shops/acme", "title": "Acme Plumbing"},
            {"link": "https://eacsociety.org/members", "title": "Acme Plumbing"},
            {"link": "https://otherco.com", "title": "Acme Plumbing"},
            {"link": "https://acmeplumbing.com", "title": "Acme Plumbing"},
        ]
    }
    domain, _title = serp._domain_from_organic(item, company_name="Acme Plumbing")
    assert domain == "acmeplumbing.com"


def test_missing_key_skips_without_http(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp.settings, "serper_api_key", "")
    calls: list[str] = []

    def boom(*_a: object, **_k: object) -> None:
        calls.append("http")
        raise AssertionError("Serper HTTP must not run without a key")

    monkeypatch.setattr(serp.http_client, "post", boom)
    out = serp.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin", "state": "TX"}],
        concurrency=1,
    )
    assert out.skipped == "serper_key_missing"
    assert out.candidates == {}
    assert out.cost_usd == 0
    assert out.billing == "serper google search"
    assert calls == []


def test_maps_results_by_array_index(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp.settings, "serper_api_key", "tok")
    posted: list[object] = []

    def fake_post(_tier: str, url: str, **kwargs: object) -> MagicMock:
        posted.append(kwargs.get("json"))
        assert url == serp.SERP_URL
        assert kwargs["headers"]["X-API-KEY"] == "tok"  # type: ignore[index]
        assert kwargs["timeout"] == 60
        return _resp(
            200,
            [
                {"organic": [{"title": "Acme", "link": "https://acme.com", "position": 1}]},
                {"organic": [{"title": "Beta", "link": "https://betaplumbing.com", "position": 1}]},
            ],
        )

    monkeypatch.setattr(serp.http_client, "post", fake_post)
    out = serp.resolve_rows(
        [
            {"_source_key": "1", "company_name": "Acme", "city": "Austin", "state": "TX"},
            {"_source_key": "2", "company_name": "Beta Plumbing", "city": "Dallas", "state": "TX"},
        ],
        unit=0.001,
        concurrency=1,
    )
    assert posted and posted[0] == [
        {"q": "Acme Austin TX", "gl": "us", "hl": "en", "num": 10},
        {"q": "Beta Plumbing Dallas TX", "gl": "us", "hl": "en", "num": 10},
    ]
    assert out.inputs_passed == ["company_name", "city", "state"]
    assert out.candidates["1"].domain == "acme.com"
    assert out.candidates["2"].domain == "betaplumbing.com"
    assert out.errored == 0


def test_pack_falls_back_to_search_parameters_q() -> None:
    packed = serp._pack_items(
        [{"key": "1", "q": "Acme Austin TX", "name": "Acme"}],
        [
            None,  # type: ignore[list-item]
            _serper_item("Acme Austin TX", "https://acme.com", "Acme"),
        ],
        unit=0.001,
    )
    assert packed == [
        {"key": "1", "domain": "acme.com", "title": "Acme", "cost_usd": 0.001}
    ]


def test_token_match_through_resolve_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp.settings, "serper_api_key", "tok")

    def fake_post(*_a: object, **_k: object) -> MagicMock:
        return _resp(
            200,
            [
                {
                    "searchParameters": {"q": "Acme Plumbing Austin TX"},
                    "organic": [
                        {"title": "Acme Plumbing", "link": "https://waze.com/place/1"},
                        {"title": "Directory", "link": "https://otherco.com"},
                        {"title": "Acme Plumbing", "link": "https://acmeplumbing.com"},
                    ],
                }
            ],
        )

    monkeypatch.setattr(serp.http_client, "post", fake_post)
    out = serp.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme Plumbing", "city": "Austin", "state": "TX"}],
        concurrency=1,
    )
    assert out.candidates["1"].domain == "acmeplumbing.com"


def test_429_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp.settings, "serper_api_key", "tok")
    monkeypatch.setattr(serp, "_retry_delay", lambda _a: 0)
    calls = {"n": 0}

    def fake_post(*_a: object, **_k: object) -> MagicMock:
        calls["n"] += 1
        if calls["n"] < 3:
            return _resp(429, {})
        return _resp(200, [_serper_item("Acme Austin TX", "https://acme.com", "Acme")])

    monkeypatch.setattr(serp.http_client, "post", fake_post)
    out = serp.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin", "state": "TX"}],
        concurrency=1,
    )
    assert calls["n"] == 3
    assert out.candidates["1"].domain == "acme.com"
    assert out.cost_usd == 0.001


def test_429_exhausted_marks_chunk_errored(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp.settings, "serper_api_key", "tok")
    monkeypatch.setattr(serp, "_retry_delay", lambda _a: 0)
    import domain_waterfall.tier_pool as pool

    monkeypatch.setattr(pool, "ROW_ERROR_RETRIES", 1)

    def fake_post(*_a: object, **_k: object) -> MagicMock:
        return _resp(429, {})

    monkeypatch.setattr(serp.http_client, "post", fake_post)
    rows = [
        {"_source_key": str(i), "company_name": f"Co{i}", "city": "Austin", "state": "TX"}
        for i in range(150)
    ]
    out = serp.resolve_rows(rows, concurrency=1)
    assert out.cost_usd == 0.0
    assert out.billed_calls == 0
    assert out.errored >= 150
    assert not out.candidates
    assert out.error


def test_cost_is_unit_times_queries_sent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp.settings, "serper_api_key", "tok")

    def fake_post(*_a: object, **kwargs: object) -> MagicMock:
        body = kwargs.get("json")
        assert isinstance(body, list)
        return _resp(
            200,
            [
                _serper_item("Acme Austin TX", "https://acme.com", "Acme"),
                _serper_item("Beta Dallas TX", "https://beta.com", "Beta"),
            ],
        )

    monkeypatch.setattr(serp.http_client, "post", fake_post)
    rows = [
        {"_source_key": "1", "company_name": "Acme", "city": "Austin", "state": "TX"},
        {"_source_key": "2", "company_name": "Beta", "city": "Dallas", "state": "TX"},
    ]
    out = serp.resolve_rows(rows, unit=0.001, concurrency=1)
    assert out.cost_usd == pytest.approx(0.002)
    assert out.billing == "serper google search"
    assert out.billed_calls == 2


def test_live_unit_price_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp.settings, "serper_api_key", "tok")
    monkeypatch.delenv("SERPER_UNIT_USD", raising=False)
    unit, info = serp.live_unit_price()
    assert unit == 0.001
    assert info["billing"] == "serper google search"
    monkeypatch.setenv("SERPER_UNIT_USD", "0.0025")
    unit, info = serp.live_unit_price()
    assert unit == 0.0025
    assert info["unit_usd"] == 0.0025


def test_default_chunk_concurrency_is_four(monkeypatch: pytest.MonkeyPatch) -> None:
    from domain_waterfall.tier_pool import resolve_tier_concurrency

    monkeypatch.delenv("TIER_CONCURRENCY", raising=False)
    monkeypatch.delenv("SERP_TIER_CONCURRENCY", raising=False)
    assert resolve_tier_concurrency("serp") == 4
    monkeypatch.setenv("SERP_TIER_CONCURRENCY", "6")
    assert resolve_tier_concurrency("serp") == 6


def test_does_not_fall_back_to_apify(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp.settings, "serper_api_key", "")
    if hasattr(serp.settings, "apify_token"):
        monkeypatch.setattr(serp.settings, "apify_token", "apify-tok")
    out = serp.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin", "state": "TX"}],
        concurrency=1,
    )
    assert out.skipped == "serper_key_missing"


def test_transport_error_logs_fields(caplog: pytest.LogCaptureFixture) -> None:
    from domain_waterfall.concurrency import VendorTransportError

    caplog.set_level("WARNING")
    text = str(
        VendorTransportError(
            "serp",
            status=504,
            message="gateway timeout",
            timeout=60,
            url="https://google.serper.dev/search",
        )
    )
    assert "status=504" in text
    assert "timeout=60s" in text
    assert "gateway timeout" in text
