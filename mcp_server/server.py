"""Domain Waterfall MCP — company name + location → trusted domain."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable

from mcp.server import MCPServer
from mcp.types import ToolAnnotations

from mcp_server.playbook import INSTRUCTIONS, WHEN_TO_USE

ROOT = Path(__file__).resolve().parent.parent

mcp = MCPServer(
    name="domain-waterfall",
    title="Domain Waterfall",
    description=(
        "Resolve a trusted company domain from a company name plus location. "
        "Never finds a person. Never finds an email. Profiles, not industry code."
    ),
    instructions=INSTRUCTIONS,
    version="0.1.0",
)


def _json(data: Any) -> str:
    return json.dumps(data, indent=2, default=str)


def _ensure_repo_cwd() -> None:
    os.chdir(ROOT)


def _http_mode() -> bool:
    return os.environ.get("MCP_TRANSPORT", "stdio").lower() in (
        "streamable-http",
        "http",
        "sse",
    )


def _reload_settings() -> None:
    from domain_waterfall import config as cfg

    cfg.settings = cfg.load_settings()


@mcp.resource(
    "domain-waterfall://playbook",
    name="playbook",
    description="When to use Domain Waterfall and what it writes.",
    mime_type="text/markdown",
)
def playbook_resource() -> str:
    return INSTRUCTIONS


@mcp.prompt(
    name="when_to_use",
    description="Decide whether the Domain Waterfall MCP applies.",
)
def when_to_use_prompt() -> str:
    return WHEN_TO_USE


@mcp.tool(
    annotations=ToolAnnotations(
        title="Health / config check",
        readOnlyHint=True,
        openWorldHint=False,
    )
)
def health() -> str:
    """Show which vendor keys and Supabase credentials are configured. Never prints secrets."""
    _ensure_repo_cwd()
    _reload_settings()
    from domain_waterfall.config import settings
    from domain_waterfall.waterfall import hydrate_keys

    keys = hydrate_keys()
    return _json(
        {
            "ok": True,
            "service": "domain-waterfall",
            "product": "domain_resolution",
            "not": ["people_finder", "email_finder"],
            "supabase_configured": settings.supabase_configured,
            "supabase_url": settings.supabase_url or None,
            "vendors": keys,
            "auth": "none",
        }
    )


@mcp.tool(
    annotations=ToolAnnotations(
        title="Ensure / upsert a client profile",
        readOnlyHint=False,
        openWorldHint=False,
        destructiveHint=False,
    )
)
def ensure_profile(client_tag: str, profile_json: Any) -> str:
    """Create or replace the JSON profile for client_tag in public.wf_client_profiles."""
    _ensure_repo_cwd()
    _reload_settings()
    from domain_waterfall.profiles import ensure_profile as _ensure

    if isinstance(profile_json, str):
        blob = json.loads(profile_json) if profile_json.strip() else {}
    elif isinstance(profile_json, dict):
        blob = profile_json
    else:
        raise ValueError("profile_json must be an object or JSON string")
    profile = _ensure(client_tag, blob)
    return _json({"ok": True, **profile.to_public()})


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get a client profile",
        readOnlyHint=True,
        openWorldHint=False,
    )
)
def get_profile(client_tag: str) -> str:
    """Read the JSON profile for client_tag. Nothing industry-specific is hardcoded."""
    _ensure_repo_cwd()
    _reload_settings()
    from domain_waterfall.profiles import get_profile as _get

    return _json({"ok": True, **_get(client_tag).to_public()})


@mcp.tool(
    annotations=ToolAnnotations(
        title="Receipt test against ground truth",
        readOnlyHint=False,
        openWorldHint=True,
        destructiveHint=False,
    )
)
def receipt_test(client_tag: str, n: int = 25) -> str:
    """Score each tier on n ground-truth rows, with and without location.

    Prints the live cost estimate first. Drops zero-yield tiers from the profile
    and writes measured hit rates plus tier_order. Counts only — no row payloads.
    """
    _ensure_repo_cwd()
    _reload_settings()
    from domain_waterfall.receipt import receipt_test as _run

    def _job(job: Any) -> dict[str, Any]:
        from mcp_server.jobs import update_job_progress

        return _run(
            client_tag,
            n=int(n or 25),
            progress=lambda snap: update_job_progress(job.id, snap),
        )

    if _http_mode():
        from mcp_server.jobs import start_job

        job = start_job("receipt_test", _job, meta={"client_tag": client_tag, "n": n})
        return _json(
            {
                "job_id": job.id,
                "status": job.status,
                "message": f"Poll get_job_status with job_id={job.id}.",
            }
        )
    return _json(_run(client_tag, n=int(n or 25)))


@mcp.tool(
    annotations=ToolAnnotations(
        title="Resolve domains from a source table",
        readOnlyHint=False,
        openWorldHint=True,
        destructiveHint=True,
    )
)
def resolve_domain(
    source_table: str,
    where: str,
    client_tag: str,
    max_tier: str = "",
    min_tier: str = "",
    skip_tiers: str = "",
    approve_cost_usd: float | None = None,
    estimate_only: bool = False,
    limit: int | None = None,
) -> str:
    """Resolve domains onto source rows. source_table + where only. Counts/cost, no rows.

    estimate_only=true returns rows per tier, live unit prices, and a dollar total.
    approve_cost_usd is the paid-tier ceiling; free tiers ignore it.
    min_tier / skip_tiers apply to the real run only (not estimate_only).
    skip_tiers is a comma-separated list (e.g. "maps").
    """
    _ensure_repo_cwd()
    _reload_settings()
    from domain_waterfall.waterfall import resolve_domain as _resolve

    if not (source_table or "").strip():
        raise ValueError("source_table is required")
    if not (client_tag or "").strip():
        raise ValueError("client_tag is required")

    def _run_resolve(
        progress: Callable[[dict[str, Any]], None] | None = None,
        should_stop: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        return _resolve(
            source_table=source_table,
            where=where or "",
            client_tag=client_tag,
            max_tier=max_tier or "",
            min_tier=min_tier or "",
            skip_tiers=skip_tiers,
            approve_cost_usd=approve_cost_usd,
            estimate_only=bool(estimate_only),
            progress=progress,
            writeback=not estimate_only,
            limit=int(limit) if limit else None,
            should_stop=should_stop,
        )

    if estimate_only:
        return _json(_run_resolve())

    def _job(job: Any) -> dict[str, Any]:
        from mcp_server.jobs import is_cancelled, update_job_progress

        return _run_resolve(
            progress=lambda snap: update_job_progress(job.id, snap),
            should_stop=lambda: is_cancelled(job.id),
        )

    if _http_mode():
        from mcp_server.jobs import start_job

        job = start_job(
            "resolve_domain",
            _job,
            meta={
                "client_tag": client_tag,
                "source_table": source_table,
                "where": where,
                "max_tier": max_tier,
                "min_tier": min_tier,
                "skip_tiers": skip_tiers,
                "limit": limit,
            },
        )
        return _json(
            {
                "job_id": job.id,
                "status": job.status,
                "message": f"Poll get_job_status with job_id={job.id}.",
            }
        )
    return _json(_run_resolve())


@mcp.tool(
    annotations=ToolAnnotations(
        title="Get job status",
        readOnlyHint=True,
        openWorldHint=False,
    )
)
def get_job_status(job_id: str) -> str:
    """Last known progress on a long job. Includes processed/targets/hits mid-tier. Never a bare error."""
    from mcp_server.jobs import get_job

    return _json(get_job(job_id))


@mcp.tool(
    annotations=ToolAnnotations(
        title="List jobs",
        readOnlyHint=True,
        openWorldHint=False,
    )
)
def list_jobs(limit: int = 20) -> str:
    """Recent domain-waterfall jobs on this process. Counts only."""
    from mcp_server.jobs import list_jobs as _list

    return _json(_list(limit=limit))


def _mount_http_routes() -> None:
    try:
        from starlette.requests import Request
        from starlette.responses import JSONResponse, PlainTextResponse
    except ImportError:
        return

    @mcp.custom_route("/", methods=["GET"])
    async def root_page(_request: Request) -> PlainTextResponse:
        return PlainTextResponse(
            "Domain Waterfall MCP\n"
            "Claude custom connector URL: /mcp\n"
            "Health: /health\n"
            "Auth: none\n"
        )

    @mcp.custom_route("/health", methods=["GET"])
    async def health_live(_request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "ok": True,
                "service": "domain-waterfall",
                "transport": "streamable-http",
                "mcp_path": "/mcp",
                "auth": "none",
            }
        )


_mount_http_routes()


def main() -> None:
    _ensure_repo_cwd()
    transport = os.environ.get("MCP_TRANSPORT", "stdio").lower()
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8000"))

    if transport in ("streamable-http", "http"):
        kwargs: dict[str, Any] = {
            "transport": "streamable-http",
            "host": host,
            "port": port,
        }
        try:
            from mcp.server.transport_security import TransportSecuritySettings

            kwargs.update(
                {
                    "streamable_http_path": "/mcp",
                    "stateless_http": True,
                    "transport_security": TransportSecuritySettings(
                        enable_dns_rebinding_protection=False
                    ),
                }
            )
        except Exception:
            kwargs["path"] = "/mcp"
        mcp.run(**kwargs)
        return
    if transport == "sse":
        mcp.run(transport="sse", host=host, port=port)
        return
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
