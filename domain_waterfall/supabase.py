"""PostgREST helpers. Writeback never touches columns this service does not own."""

from __future__ import annotations

import json
from typing import Any
from urllib import error, request

from .config import load_settings

WRITE_COLUMNS = (
    "wf_domain",
    "wf_domain_source",
    "wf_domain_confidence",
    "wf_domain_agreement",
    "wf_domain_candidates",
    "wf_phone",
    "wf_domain_status",
)
FORBIDDEN_WRITE = frozenset({"dl_status", "sg_exclude"})


def supabase_config() -> dict[str, str]:
    cfg = load_settings()
    if not cfg.supabase_configured:
        raise RuntimeError(
            "Supabase not configured. Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY."
        )
    return {"url": cfg.supabase_url.rstrip("/"), "key": cfg.supabase_key}


def _headers(key: str, *, prefer: str) -> dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": prefer,
        "Accept": "application/json",
    }


def request_on(
    method: str,
    path: str,
    *,
    url: str,
    key: str,
    body: Any = None,
    prefer: str = "return=minimal",
    extra_headers: dict[str, str] | None = None,
) -> tuple[int, str]:
    endpoint = f"{url.rstrip('/')}/rest/v1/{path}"
    data = None if body is None else json.dumps(body).encode("utf-8")
    headers = _headers(key, prefer=prefer)
    if extra_headers:
        headers.update(extra_headers)
    req = request.Request(endpoint, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=120) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Supabase {method} {endpoint} failed ({exc.code}): {detail[:500]}"
        ) from exc


def request(
    method: str,
    path: str,
    *,
    body: Any = None,
    prefer: str = "return=minimal",
) -> tuple[int, str]:
    cfg = supabase_config()
    return request_on(method, path, url=cfg["url"], key=cfg["key"], body=body, prefer=prefer)


def rpc(name: str, body: dict[str, Any] | None = None) -> Any:
    _status, text = request(
        "POST",
        f"rpc/{name}",
        body=body or {},
        prefer="return=representation",
    )
    if not text:
        return None
    try:
        return json.loads(text)
    except ValueError:
        return text


def filter_write_fields(fields: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, val in fields.items():
        if key.startswith("skip_"):
            continue
        if key in FORBIDDEN_WRITE:
            continue
        if key not in WRITE_COLUMNS:
            continue
        out[key] = val
    return out
