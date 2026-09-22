from domain_waterfall.waterfall import make_row_ticker
from domain_waterfall.vendors.base import report_progress


def test_report_progress_noop_without_callback() -> None:
    report_progress(None, 1, 10, 0)


def test_row_ticker_emits_first_last_and_throttles() -> None:
    clock = {"now": 0.0}
    seen: list[dict] = []

    def emit(extra: dict) -> None:
        seen.append(dict(extra))

    tick = make_row_ticker(emit, min_interval_s=2.0, clock=lambda: clock["now"])
    tick(1, 10, 0)
    clock["now"] = 0.5
    tick(2, 10, 1)
    clock["now"] = 2.1
    tick(5, 10, 2)
    tick(10, 10, 3)

    assert [s["processed"] for s in seen] == [1, 5, 10]
    assert seen[-1]["hits"] == 3
    assert seen[-1]["targets"] == 10
    assert seen[-1]["rows_attempted"] == 10
    assert "last_progress_at" in seen[-1]


def test_row_ticker_passes_serp_run_ids() -> None:
    seen: list[dict] = []
    tick = make_row_ticker(seen.append, min_interval_s=0.0)
    tick(0, 10, 0, {"serp_run_ids": ["abc"], "tier_cost_usd": 0.45, "last_progress_at": "t1"})
    assert seen[-1]["serp_run_ids"] == ["abc"]
    assert seen[-1]["tier_cost_usd"] == 0.45
    assert seen[-1]["last_progress_at"] == "t1"
