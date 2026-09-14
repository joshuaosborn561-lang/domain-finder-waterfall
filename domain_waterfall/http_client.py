"""Shared gated HTTP helpers for vendor clients."""

from __future__ import annotations

import requests

from .concurrency import request_with_retry

TimeoutArg = float | int | tuple[float, float]


def post(
    tier: str,
    url: str,
    *,
    json: object | None = None,
    data: object | None = None,
    files: object | None = None,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
    timeout: TimeoutArg = 45,
    max_attempts: int | None = None,
    acquire_timeout: float | None = None,
) -> requests.Response | None:
    kwargs: dict[str, object] = {
        "json": json,
        "data": data,
        "files": files,
        "headers": headers,
        "params": params,
        "timeout": timeout,
    }
    if max_attempts is not None:
        kwargs["max_attempts"] = max_attempts
    if acquire_timeout is not None:
        kwargs["acquire_timeout"] = acquire_timeout
    return request_with_retry(tier, "POST", url, **kwargs)


def get(
    tier: str,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, str] | None = None,
    timeout: TimeoutArg = 45,
    max_attempts: int | None = None,
    acquire_timeout: float | None = None,
) -> requests.Response | None:
    kwargs: dict[str, object] = {
        "headers": headers,
        "params": params,
        "timeout": timeout,
    }
    if max_attempts is not None:
        kwargs["max_attempts"] = max_attempts
    if acquire_timeout is not None:
        kwargs["acquire_timeout"] = acquire_timeout
    return request_with_retry(tier, "GET", url, **kwargs)
