import pytest

from domain_waterfall.source import parse_source, where_to_filters
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


def test_request_helper_does_not_shadow_urllib() -> None:
    from domain_waterfall import supabase as sb

    assert hasattr(sb.urllib_request, "Request")
    assert callable(sb.request)


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
