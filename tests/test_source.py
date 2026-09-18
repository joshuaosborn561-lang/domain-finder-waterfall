import pytest

from domain_waterfall.source import (
    FetchResult,
    _coerce_columns,
    apply_column_overrides,
    describe_count_sql,
    discover_column_map,
    parse_source,
    where_to_filters,
)
from domain_waterfall.supabase import filter_write_fields


def test_where_in_and_null() -> None:
    filters = where_to_filters("lane = 'commercial_gc' AND domain is null AND city in ('Dallas','Plano')")
    assert filters[0] == {"col": "lane", "op": "eq", "value": "commercial_gc"}
    assert filters[1] == {"col": "domain", "op": "is null"}
    assert filters[2]["op"] == "in"
    assert filters[2]["value"] == ["Dallas", "Plano"]


def test_where_rejects_or() -> None:
    with pytest.raises(ValueError):
        where_to_filters("domain is null OR lane = 'x'")


def test_parse_source_table() -> None:
    src = parse_source("client_peterson.gc_adjudication", "domain is null")
    assert src.schema == "client_peterson"
    assert src.table == "gc_adjudication"


def test_parse_source_does_not_double_qualify() -> None:
    bare = parse_source("emcor_needs_domain", "domain is null")
    qualified = parse_source("public.emcor_needs_domain", "domain is null")
    assert bare.schema == qualified.schema == "public"
    assert bare.table == qualified.table == "emcor_needs_domain"
    assert bare.qualified == "public.emcor_needs_domain"


def test_discover_uses_place_id(monkeypatch: pytest.MonkeyPatch) -> None:
    src = parse_source("emcor_needs_domain", "domain is null")
    apply_column_overrides(
        src,
        {
            "name_column": "name",
            "city_column": "city",
            "state_column": "state",
            "domain_column": "domain",
        },
    )
    cols = {
        "place_id",
        "name",
        "city",
        "state",
        "zip",
        "address",
        "phone",
        "domain",
        "dl_status",
    }
    monkeypatch.setattr("domain_waterfall.source.list_columns", lambda _src: cols)
    discover_column_map(src)
    assert src.key_column == "place_id"
    assert src.column_map["company_name"] == "name"
    assert src.column_map["city"] == "city"
    assert "id" not in src.column_map.values()
    sql = describe_count_sql(src)
    assert sql == "SELECT count(*) FROM public.emcor_needs_domain WHERE true AND domain IS NULL"


def test_discover_raises_without_key(monkeypatch: pytest.MonkeyPatch) -> None:
    src = parse_source("no_key_table", "domain is null")
    monkeypatch.setattr("domain_waterfall.source.list_columns", lambda _src: {"foo", "bar"})
    with pytest.raises(ValueError, match="no usable key column"):
        discover_column_map(src)


def test_coerce_columns_shapes() -> None:
    assert _coerce_columns(["place_id", "name"]) == {"place_id", "name"}
    assert _coerce_columns("{place_id,name,city}") == {"place_id", "name", "city"}
    assert _coerce_columns({"dw_source_columns": ["place_id"]}) == {"place_id"}


def test_request_helper_does_not_shadow_urllib() -> None:
    from domain_waterfall import supabase as sb

    assert hasattr(sb.urllib_request, "Request")
    assert callable(sb.request)


def test_fetch_result_fails_on_unexplained_loss() -> None:
    ok = FetchResult(
        rows=[{}],
        rows_matched=10,
        rows_fetched=8,
        rows_excluded=2,
        exclusion_reasons={"job_limit": 2},
    )
    ok.assert_explained()
    bad = FetchResult(
        rows=[{}],
        rows_matched=10,
        rows_fetched=8,
        rows_excluded=2,
        exclusion_reasons={"job_limit": 1},
    )
    with pytest.raises(ValueError, match="unexplained row loss"):
        bad.assert_explained()


def test_write_allowlist() -> None:
    out = filter_write_fields(
        {
            "wf_domain": "x.com",
            "dl_status": "skip",
            "sg_exclude": True,
            "skip_reason": "no",
            "email": "a@b.com",
            "wf_phone": "214",
        }
    )
    assert set(out) == {"wf_domain", "wf_phone"}
    assert "dl_status" not in out
