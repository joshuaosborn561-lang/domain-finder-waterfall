"""Client profiles live in public.wf_client_profiles. Nothing industry-specific in code."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from . import supabase as sb
from .gate import DEFAULT_TLDS, ProfileGate
from .pricing import DEFAULT_PIPELINE

DEFAULT_STRIP = [
    "inc",
    "llc",
    "ltd",
    "co",
    "corp",
    "company",
    "group",
    "services",
    "construction",
    "contractors",
    "builders",
]


@dataclass
class ClientProfile:
    client_tag: str
    display_name: str = ""
    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def geo(self) -> dict[str, Any]:
        geo = self.raw.get("geo") or {}
        return geo if isinstance(geo, dict) else {}

    @property
    def country(self) -> str:
        return str(self.geo.get("country") or "US")

    @property
    def states(self) -> list[str]:
        return [str(s) for s in (self.geo.get("states") or []) if s]

    @property
    def cities(self) -> list[str]:
        return [str(s) for s in (self.geo.get("cities") or []) if s]

    @property
    def area_codes(self) -> list[str]:
        return [str(s) for s in (self.geo.get("area_codes") or []) if s]

    @property
    def center_lat(self) -> float | None:
        val = self.geo.get("center_lat")
        return float(val) if val not in (None, "") else None

    @property
    def center_lng(self) -> float | None:
        val = self.geo.get("center_lng")
        return float(val) if val not in (None, "") else None

    @property
    def radius_mi(self) -> float | None:
        val = self.geo.get("radius_mi")
        return float(val) if val not in (None, "") else None

    @property
    def geo_required(self) -> bool:
        return bool(self.geo.get("geo_required", True))

    @property
    def geo_in_query(self) -> bool:
        return bool(self.geo.get("geo_in_query", False))

    @property
    def min_confidence(self) -> float:
        val = self.raw.get("min_confidence", 0.25)
        try:
            return float(val)
        except (TypeError, ValueError):
            return 0.25

    @property
    def explicit_tier_order(self) -> list[str]:
        raw = self.raw.get("tier_order")
        if not isinstance(raw, list):
            return []
        out: list[str] = []
        for item in raw:
            if isinstance(item, str) and item.strip():
                out.append(item.strip())
            elif isinstance(item, dict) and item.get("tier"):
                out.append(str(item["tier"]).strip())
        return out

    @property
    def tier_order_frozen(self) -> bool:
        return bool(self.raw.get("tier_order_frozen", False))

    @property
    def name_strip_tokens(self) -> list[str]:
        tokens = self.raw.get("name_strip_tokens")
        if not tokens:
            return list(DEFAULT_STRIP)
        return [str(t) for t in tokens]

    @property
    def industry_reject_regex(self) -> str | None:
        val = self.raw.get("industry_reject_regex")
        return str(val) if val else None

    @property
    def industry_allow_regex(self) -> str | None:
        val = self.raw.get("industry_allow_regex")
        return str(val) if val else None

    @property
    def aggregator_blocklist(self) -> list[str]:
        return [str(x) for x in (self.raw.get("aggregator_blocklist") or []) if x]

    @property
    def tld_allow(self) -> list[str]:
        raw = self.raw.get("tld_allow")
        if not raw:
            return list(DEFAULT_TLDS)
        return [str(t) if str(t).startswith(".") else f".{t}" for t in raw]

    @property
    def ground_truth(self) -> dict[str, Any]:
        gt = self.raw.get("ground_truth") or {}
        return gt if isinstance(gt, dict) else {}

    @property
    def cache_tables(self) -> list[str]:
        # People waterfall already uses cache_tables for contact tables.
        # Domain resolver prefers domain_cache_tables so the two do not clobber.
        raw = self.raw.get("domain_cache_tables")
        if raw is None:
            raw = self.raw.get("cache_tables")
        return [str(t) for t in (raw or []) if t]

    @property
    def second_opinion(self) -> bool:
        return bool(self.raw.get("second_opinion", False))

    @property
    def enabled_tiers(self) -> list[str]:
        raw = self.raw.get("enabled_tiers")
        if isinstance(raw, list) and raw:
            return [str(t) for t in raw]
        dropped = set(self.raw.get("dropped_tiers") or [])
        return [t for t in DEFAULT_PIPELINE if t not in dropped]

    @property
    def dropped_tiers(self) -> list[str]:
        return [str(t) for t in (self.raw.get("dropped_tiers") or [])]

    @property
    def hit_rates(self) -> dict[str, float]:
        raw = self.raw.get("hit_rates") or {}
        if not isinstance(raw, dict):
            return {}
        out: dict[str, float] = {}
        for key, val in raw.items():
            try:
                out[str(key)] = float(val)
            except (TypeError, ValueError):
                continue
        return out

    @property
    def name_reject_regex(self) -> str | None:
        val = self.raw.get("name_reject_regex")
        return str(val) if val else None

    def gate(self) -> ProfileGate:
        return ProfileGate(
            name_strip_tokens=self.name_strip_tokens,
            industry_reject_regex=self.industry_reject_regex,
            industry_allow_regex=self.industry_allow_regex,
            aggregator_blocklist=self.aggregator_blocklist,
            tld_allow=self.tld_allow,
            area_codes=self.area_codes,
            states=self.states,
            geo_required=self.geo_required,
            min_confidence=self.min_confidence,
        )

    def to_public(self) -> dict[str, Any]:
        return {
            "client_tag": self.client_tag,
            "display_name": self.display_name,
            "profile": self.raw,
        }


def deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    """Nested dict merge. Lists and scalars from overlay win. Keeps people-waterfall keys."""
    out = dict(base)
    for key, val in overlay.items():
        if isinstance(val, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], val)
        else:
            out[key] = val
    return out


def _row_to_profile(row: dict[str, Any]) -> ClientProfile:
    blob = row.get("profile") or row.get("profile_json") or {}
    if isinstance(blob, str):
        blob = json.loads(blob)
    if not isinstance(blob, dict):
        blob = {}
    tag = str(row.get("client_tag") or blob.get("client_tag") or "")
    return ClientProfile(
        client_tag=tag,
        display_name=str(row.get("display_name") or blob.get("display_name") or tag),
        raw=blob,
    )


def get_profile(client_tag: str) -> ClientProfile:
    tag = (client_tag or "").strip().lower()
    if not tag:
        raise ValueError("client_tag is required")
    data = sb.rpc("dw_get_profile", {"p_client_tag": tag})
    if isinstance(data, list):
        data = data[0] if data else None
    if isinstance(data, dict) and data.get("dw_get_profile"):
        data = data["dw_get_profile"]
    if not data:
        raise ValueError(f"unknown client_tag {tag!r}")
    if isinstance(data, str):
        data = json.loads(data)
    return _row_to_profile(data if isinstance(data, dict) else {"client_tag": tag, "profile": {}})


def ensure_profile(client_tag: str, profile_json: dict[str, Any] | str) -> ClientProfile:
    tag = (client_tag or "").strip().lower()
    if not tag:
        raise ValueError("client_tag is required")
    if isinstance(profile_json, str):
        blob = json.loads(profile_json) if profile_json.strip() else {}
    else:
        blob = dict(profile_json)
    blob["client_tag"] = tag
    try:
        existing = get_profile(tag)
        blob = deep_merge(existing.raw, blob)
        display = str(blob.get("display_name") or existing.display_name or tag)
    except ValueError:
        display = str(blob.get("display_name") or tag)
    data = sb.rpc(
        "dw_ensure_profile",
        {"p_client_tag": tag, "p_display_name": display, "p_profile": blob},
    )
    if isinstance(data, list):
        data = data[0] if data else None
    if isinstance(data, dict) and data.get("dw_ensure_profile"):
        data = data["dw_ensure_profile"]
    if isinstance(data, str):
        data = json.loads(data)
    if isinstance(data, dict) and data:
        return _row_to_profile(data)
    return ClientProfile(client_tag=tag, display_name=display, raw=blob)


def update_profile_fields(client_tag: str, patch: dict[str, Any]) -> ClientProfile:
    current = get_profile(client_tag)
    merged = deep_merge(current.raw, patch)
    merged["client_tag"] = current.client_tag
    if "display_name" not in patch:
        merged["display_name"] = current.display_name
    return ensure_profile(current.client_tag, merged)
