from unittest.mock import MagicMock

import pytest

from domain_waterfall.concurrency import VendorTransportError
from domain_waterfall.vendors import serp


def _resp(status: int, payload: object, text: str = "") -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = text
    r.json.return_value = payload
    return r


def test_poll_budget_covers_slow_batch() -> None:
    # One live query is 45s plus. 40 * 3s = 120s used to lose every 100-query run.
    assert serp.poll_budget_s(1) >= 900
    assert serp.poll_budget_s(20) == 20 * 50
    assert serp.poll_budget_s(100) == serp.SERP_POLL_CAP_S
    assert serp.poll_budget_s(1000) == serp.SERP_POLL_CAP_S


def test_start_run_is_async_no_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    posted: list[str] = []

    def fake_post(_tier: str, url: str, **_k: object) -> MagicMock:
        posted.append(url)
        return _resp(201, {"data": {"id": "run123", "status": "RUNNING"}})

    monkeypatch.setattr(serp.settings, "apify_token", "tok")
    monkeypatch.setattr(serp.http_client, "post", fake_post)
    run_id = serp._start_run([{"q": '"Acme" Austin TX'}])
    assert run_id == "run123"
    assert posted
    assert "waitForFinish" not in posted[0]
    assert posted[0].endswith("/runs")


def test_poll_waits_until_succeeded(monkeypatch: pytest.MonkeyPatch) -> None:
    states = iter(
        [
            _resp(200, {"data": {"status": "RUNNING", "defaultDatasetId": "ds1"}}),
            _resp(200, {"data": {"status": "SUCCEEDED", "defaultDatasetId": "ds1"}}),
            _resp(
                200,
                [
                    {
                        "searchQuery": {"term": '"Acme" Austin TX'},
                        "organicResults": [{"url": "https://acme.com", "title": "Acme"}],
                    }
                ],
            ),
        ]
    )

    def fake_get(_tier: str, url: str, **_k: object) -> MagicMock:
        return next(states)

    monkeypatch.setattr(serp.http_client, "get", fake_get)
    monkeypatch.setattr(serp, "SERP_POLL_INTERVAL_S", 0.0)
    items = serp._poll("run123", n_queries=1)
    assert items and items[0]["organicResults"][0]["url"] == "https://acme.com"


def test_poll_timeout_includes_status_and_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(_tier: str, url: str, **_k: object) -> MagicMock:
        return _resp(200, {"data": {"status": "RUNNING", "defaultDatasetId": "ds1"}})

    monkeypatch.setattr(serp.http_client, "get", fake_get)
    monkeypatch.setattr(serp, "SERP_POLL_INTERVAL_S", 0.0)
    monkeypatch.setattr(serp, "poll_budget_s", lambda _n: 0.0)
    with pytest.raises(VendorTransportError) as excinfo:
        serp._poll("run123", n_queries=100)
    err = excinfo.value
    assert err.status == "RUNNING"
    assert err.timeout == 0.0
    assert "poll timeout" in str(err)
    assert "status=RUNNING" in str(err)
    assert "timeout=" in str(err)


def test_poll_failed_run_surfaces_status(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(_tier: str, url: str, **_k: object) -> MagicMock:
        return _resp(
            200,
            {"data": {"status": "FAILED", "statusMessage": "actor crashed", "defaultDatasetId": "ds1"}},
        )

    monkeypatch.setattr(serp.http_client, "get", fake_get)
    monkeypatch.setattr(serp, "SERP_POLL_INTERVAL_S", 0.0)
    with pytest.raises(VendorTransportError) as excinfo:
        serp._poll("run123", n_queries=1)
    assert excinfo.value.status == "FAILED"
    assert "actor crashed" in str(excinfo.value)


def test_resolve_rows_batches_and_does_not_bill_on_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    starts: list[int] = []

    def fake_start(queries: list[dict[str, str]]) -> str:
        starts.append(len(queries))
        raise VendorTransportError("serp", status="timeout", message="start timeout", timeout=30)

    monkeypatch.setattr(serp.settings, "apify_token", "tok")
    monkeypatch.setattr(serp, "cache_schema", lambda: {})
    monkeypatch.setattr(serp, "_start_run", fake_start)
    monkeypatch.setattr(serp, "SERP_CHUNK", 100)
    rows = [
        {"_source_key": str(i), "company_name": f"Co{i}", "city": "Austin", "state": "TX"}
        for i in range(150)
    ]
    # Fail fast: one attempt per chunk.
    import domain_waterfall.tier_pool as pool

    monkeypatch.setattr(pool, "ROW_ERROR_RETRIES", 1)
    out = serp.resolve_rows(rows, concurrency=1)
    assert starts == [100, 50]
    assert out.cost_usd == 0.0
    assert out.billed_calls == 0
    assert out.errored >= 150
    assert not out.candidates
    assert out.error and "status=timeout" in out.error


def test_search_query_object_matches_row(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(serp.settings, "apify_token", "tok")
    monkeypatch.setattr(serp, "cache_schema", lambda: {})
    monkeypatch.setattr(serp, "_start_run", lambda _q: "run1")
    monkeypatch.setattr(
        serp,
        "_poll",
        lambda *_a, **_k: [
            {
                "searchQuery": {"term": '"Acme" Austin TX'},
                "organicResults": [{"url": "https://acme.com", "title": "Acme"}],
            }
        ],
    )
    out = serp.resolve_rows(
        [{"_source_key": "1", "company_name": "Acme", "city": "Austin", "state": "TX"}],
        concurrency=1,
    )
    assert "1" in out.candidates
    assert out.candidates["1"].domain == "acme.com"
    assert out.cost_usd > 0
    assert out.errored == 0


def test_transport_error_logs_fields(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level("WARNING")
    err = VendorTransportError(
        "serp",
        status=504,
        message="gateway timeout",
        timeout=30,
        url="https://api.apify.com/v2/acts/apify~google-search-scraper/runs",
    )
    text = str(err)
    assert "status=504" in text
    assert "timeout=30s" in text
    assert "gateway timeout" in text
    assert any("status=504" in r.message for r in caplog.records)
