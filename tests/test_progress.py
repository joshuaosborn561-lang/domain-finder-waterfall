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
