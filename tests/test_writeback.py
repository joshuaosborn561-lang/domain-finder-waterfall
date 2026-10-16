import pytest

from domain_waterfall.source import (
    PartialWriteError,
    parse_source,
    patch_source_rows,
    preflight_write_columns,
)


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
    items = [
        (
            i,
            {
                "wf_domain": "x.com",
                "wf_domain_source": "maps",
                "wf_domain_confidence": 0.85,
                "wf_domain_agreement": False,
                "wf_domain_candidates": None,
                "wf_phone": None,
                "wf_domain_status": "resolved",
            },
        )
        for i in range(1200)
    ]
    with pytest.raises(PartialWriteError, match="batch 2 failed") as excinfo:
        patch_source_rows(src, items, concurrency=4)
    assert len(calls) == 3
    assert sorted(len(c) for c in calls) == [200, 500, 500]
    written = {int(k) for k in excinfo.value.written_keys}
    assert written == set(range(500)) | set(range(1000, 1200))
    assert set(range(500, 1000)).isdisjoint(written)
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
    written = patch_source_rows(src, items, concurrency=1)
    assert written == ["0", "1", "2"]
    assert per_row == ["0", "1", "2"]
