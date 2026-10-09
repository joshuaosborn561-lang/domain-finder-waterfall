"""Live unit prices → cheapest-first tier order. Receipt never reorders; it records rates."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Published ranges as of 2026-09-09. Live account rates replace these at job start.
PUBLISHED: dict[str, dict[str, Any]] = {
    "cache": {"unit": 0.0, "always_billed": False, "free": True, "free_on_miss": False},
    "maps": {"unit": 0.0, "always_billed": False, "free": True, "free_on_miss": False},
    "aiark": {
        "unit": 0.0005,
        "unit_low": 0.0002,
        "unit_high": 0.00098,
        "credits": 0.1,
        "always_billed": True,
        "free": False,
        "free_on_miss": False,
    },
    "discolike": {
        "unit": 0.00425,
        "always_billed": True,
        "free": False,
        "free_on_miss": False,
    },
    "serp": {
        "unit": 0.0045,
        "always_billed": True,
        "free": False,
        "free_on_miss": False,
    },
    "prospeo": {
        "unit": 0.015,
        "unit_low": 0.007,
        "unit_high": 0.039,
        "credits": 1,
        "always_billed": False,
        "free": False,
        "free_on_miss": True,
    },
}

CANDIDATE_TIERS = ("texas_comptroller",)
OUT_BY_DESIGN = ("fullenrich", "hunter", "llm", "pdl")
# Josh dropped LeadMagic company-search on 2026-10-08. No replacement.
REMOVED_TIERS = frozenset({"leadmagic"})
LEGACY_REMOVED_WARNING = (
    "legacy 'leadmagic' in a profile or request tier_order is a no-op "
    "(Josh dropped LeadMagic company-search on 2026-10-08; no replacement)"
)
DEFAULT_PIPELINE = ("cache", "maps", "aiark", "discolike", "serp", "prospeo")


def is_removed_tier(name: str) -> bool:
    return (name or "").strip().lower() in REMOVED_TIERS


def iter_tier_names(raw: Any) -> list[str]:
    """Flatten strings, CSV, JSON lists, or {tier: ...} objects into names."""
    if raw is None or raw is False:
        return []
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        if text.startswith("["):
            import json

            try:
                return iter_tier_names(json.loads(text))
            except ValueError:
                pass
        return [p.strip() for p in text.split(",") if p.strip()]
    if isinstance(raw, dict):
        tier = raw.get("tier")
        return [str(tier).strip()] if tier else []
    if isinstance(raw, (list, tuple, set)):
        out: list[str] = []
        for item in raw:
            out.extend(iter_tier_names(item))
        return out
    text = str(raw).strip()
    return [text] if text else []


def strip_removed_tiers(names: Any) -> tuple[list[str], bool]:
    """Drop removed tiers. Returns (kept, saw_legacy). Does not reject the request."""
    kept: list[str] = []
    saw = False
    for name in iter_tier_names(names):
        if is_removed_tier(name):
            saw = True
            continue
        kept.append(name)
    return kept, saw


def collect_legacy_tier_warnings(*sources: Any, **request_tiers: Any) -> list[str]:
    """Warn when a profile or request still names a dropped tier."""
    names: list[str] = []
    for src in sources:
        names.extend(iter_tier_names(src))
    for key in ("min_tier", "max_tier", "skip_tiers", "tier_order"):
        if key in request_tiers:
            names.extend(iter_tier_names(request_tiers[key]))
    if any(is_removed_tier(n) for n in names):
        return [LEGACY_REMOVED_WARNING]
    return []


@dataclass
class TierPrice:
    name: str
    unit: float
    sort_cost: float
    always_billed: bool
    free: bool
    free_on_miss: bool
    hit_rate: float | None = None
    credits: float = 0.0
    source: str = "published"


@dataclass
class TierOrder:
    tiers: list[str]
    prices: dict[str, TierPrice] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    def as_profile(self) -> list[dict[str, Any]]:
        out = []
        for name in self.tiers:
            price = self.prices[name]
            out.append(
                {
                    "tier": name,
                    "unit": price.unit,
                    "sort_cost": price.sort_cost,
                    "hit_rate": price.hit_rate,
                    "free_on_miss": price.free_on_miss,
                    "always_billed": price.always_billed,
                    "source": price.source,
                }
            )
        return out


def _hit_rate(name: str, measured: dict[str, float]) -> float | None:
    if name in measured:
        return measured[name]
    return None


def sort_cost(name: str, unit: float, measured: dict[str, float]) -> float:
    meta = PUBLISHED.get(name, {})
    if meta.get("free"):
        return 0.0
    rate = _hit_rate(name, measured)
    if meta.get("free_on_miss"):
        expected = rate if rate is not None else 0.5
        return unit * expected
    return unit


def compute_order(
    enabled: list[str],
    *,
    live_units: dict[str, float] | None = None,
    measured_hit_rates: dict[str, float] | None = None,
    dropped: list[str] | None = None,
    explicit_order: list[str] | None = None,
) -> TierOrder:
    live_units = live_units or {}
    measured = measured_hit_rates or {}
    skip = set(dropped or [])
    prices: dict[str, TierPrice] = {}
    names: list[str] = []
    saw_legacy = False
    for name in enabled:
        if is_removed_tier(name):
            saw_legacy = True
            continue
        if name in skip or name in OUT_BY_DESIGN:
            continue
        meta = PUBLISHED.get(name, {"unit": 0.0, "always_billed": True, "free": False})
        unit = float(live_units.get(name, meta.get("unit", 0.0)))
        rate = _hit_rate(name, measured)
        cost = sort_cost(name, unit, measured)
        prices[name] = TierPrice(
            name=name,
            unit=unit,
            sort_cost=cost,
            always_billed=bool(meta.get("always_billed")),
            free=bool(meta.get("free")),
            free_on_miss=bool(meta.get("free_on_miss")),
            hit_rate=rate,
            credits=float(meta.get("credits") or 0),
            source="live" if name in live_units else "published",
        )
        names.append(name)

    def _key(n: str) -> tuple[float, float, str]:
        p = prices[n]
        # Free first (sort_cost 0), then paid by sort_cost, ties on measured hit rate desc.
        hit = p.hit_rate if p.hit_rate is not None else 0.0
        return (p.sort_cost, -hit, n)

    if explicit_order:
        explicit_kept, explicit_legacy = strip_removed_tiers(explicit_order)
        saw_legacy = saw_legacy or explicit_legacy
        ordered = [n for n in explicit_kept if n in prices]
        ordered += [n for n in names if n not in ordered]
        names = ordered
    else:
        names.sort(key=_key)
    warnings = [LEGACY_REMOVED_WARNING] if saw_legacy else []
    return TierOrder(tiers=names, prices=prices, warnings=warnings)


def estimate_rows(n: int, order: TierOrder, *, paid_only: bool = False) -> dict[str, Any]:
    per_tier: list[dict[str, Any]] = []
    total = 0.0
    for name in order.tiers:
        price = order.prices[name]
        if paid_only and (price.free or price.unit <= 0):
            cost = 0.0
        elif price.free_on_miss:
            rate = price.hit_rate if price.hit_rate is not None else 0.5
            cost = n * price.unit * rate
        else:
            cost = n * price.unit
        total += cost
        per_tier.append(
            {
                "tier": name,
                "rows": n,
                "unit_usd": price.unit,
                "estimated_usd": round(cost, 6),
                "free": price.free,
                "free_on_miss": price.free_on_miss,
                "hit_rate": price.hit_rate,
            }
        )
    return {"rows": n, "tiers": per_tier, "estimated_usd": round(total, 4)}
