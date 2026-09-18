import pytest

from mcp.server.mcpserver.exceptions import ToolError

from mcp_server.server import get_job_status, mcp, resolve_domain


def test_tool_names() -> None:
    import asyncio
    import inspect

    tools = mcp.list_tools()
    if inspect.iscoroutine(tools):
        tools = asyncio.run(tools)
    names = sorted(t.name for t in tools)
    assert names == [
        "cancel_job",
        "ensure_profile",
        "get_job_status",
        "get_profile",
        "health",
        "list_jobs",
        "receipt_test",
        "resolve_domain",
    ]
    banned = {"enrich_waterfall", "find_email", "find_people", "scrape_maps"}
    assert banned.isdisjoint(set(names))


def test_resolve_has_no_inline_rows() -> None:
    import inspect

    params = inspect.signature(resolve_domain).parameters
    assert "source_table" in params
    assert "where" in params
    assert "rows" not in params
    assert "estimate_only" in params
    assert "approve_cost_usd" in params
    assert "min_tier" in params
    assert "skip_tiers" in params
    assert "concurrency" in params
    assert "limit" in params


def test_get_job_status_never_raises() -> None:
    payload = get_job_status("")
    assert "unknown" in payload or "job_id is required" in payload
    payload = get_job_status("does-not-exist")
    assert "status" in payload


def test_resolve_domain_surfaces_exception() -> None:
    with pytest.raises(ToolError, match="source_table is required") as excinfo:
        resolve_domain(source_table="", where="domain is null", client_tag="emcor", estimate_only=True)
    assert "Error executing tool" not in str(excinfo.value) or "source_table is required" in str(excinfo.value)
    assert "source_table is required" in str(excinfo.value)


def test_resolve_domain_surfaces_runtime_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(**_kwargs):
        raise RuntimeError('column "id" does not exist')

    monkeypatch.setattr("domain_waterfall.waterfall.resolve_domain", boom)
    with pytest.raises(ToolError, match='column "id" does not exist') as excinfo:
        resolve_domain(
            source_table="emcor_needs_domain",
            where="domain is null",
            client_tag="emcor",
            estimate_only=True,
        )
    assert "RuntimeError" in str(excinfo.value)
