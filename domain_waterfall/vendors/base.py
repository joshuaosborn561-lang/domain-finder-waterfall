from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

OnProgress = Callable[[int, int, int], None]


def report_progress(
    on_progress: OnProgress | None, processed: int, total: int, hits: int
) -> None:
    if on_progress:
        on_progress(processed, total, hits)


@dataclass
class DomainCandidate:
    domain: str
    vendor_name: str = ""
    title: str = ""
    phone: str = ""
    address_state: str = ""
    address_city: str = ""
    place_id: str = ""
    raw: dict[str, Any] = field(default_factory=dict)
    inputs_passed: list[str] = field(default_factory=list)
    billed: bool = False
    cost_usd: float = 0.0
    credits: float = 0.0


@dataclass
class TierResult:
    tier: str
    candidates: dict[str, DomainCandidate] = field(default_factory=dict)
    none: int = 0
    calls: int = 0
    billed_calls: int = 0
    cost_usd: float = 0.0
    credits: float = 0.0
    inputs_passed: list[str] = field(default_factory=list)
    error: str | None = None
    skipped: str | None = None
