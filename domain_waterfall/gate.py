"""Acceptance gate. Every vendor output passes this or does not count."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable

from .normalize import area_code, distinctive_tokens, extract_domain, normalize_state, tld_of

GLOBAL_BLOCKLIST = frozenset(
    {
        "yelp",
        "bbb",
        "buildzoom",
        "houzz",
        "facebook",
        "linkedin",
        "indeed",
        "zoominfo",
        "manta",
        "mapquest",
        "yellowpages",
        "angi",
        "homeadvisor",
        "thumbtack",
        "dnb",
        "opencorporates",
        "bizapedia",
        "procore",
        "levelset",
        "glassdoor",
        "crunchbase",
        "google",
        "apple",
        "wikipedia",
    }
)

DEFAULT_TLDS = (".com", ".net", ".org", ".us", ".co", ".io", ".biz")
SINK_THRESHOLD = 3


@dataclass
class GateResult:
    accepted: bool
    domain: str = ""
    reason: str = ""
    token_hit: bool = False
    token_on_vendor_name: bool = False
    geo_ok: bool = False
    geo_signal: bool = False
    confidence: float = 0.0
    status: str = ""  # resolved | review | ""


@dataclass
class ProfileGate:
    name_strip_tokens: list[str] = field(default_factory=list)
    industry_reject_regex: str | None = None
    industry_allow_regex: str | None = None
    aggregator_blocklist: list[str] = field(default_factory=list)
    tld_allow: list[str] = field(default_factory=lambda: list(DEFAULT_TLDS))
    area_codes: list[str] = field(default_factory=list)
    states: list[str] = field(default_factory=list)
    geo_required: bool = True

    @property
    def blocklist(self) -> set[str]:
        extra = {b.strip().lower() for b in self.aggregator_blocklist if b}
        return set(GLOBAL_BLOCKLIST) | extra


def _compile(pattern: str | None) -> re.Pattern[str] | None:
    if not pattern:
        return None
    return re.compile(pattern, re.I)


def _blob(*parts: str) -> str:
    return " ".join(p for p in parts if p)


def is_blocklisted(domain: str, blocklist: Iterable[str]) -> bool:
    host = extract_domain(domain)
    if not host:
        return True
    if host.endswith(".gov") or host.endswith(".gov.uk"):
        return True
    labels = host.split(".")
    blocked = {b.lower() for b in blocklist}
    return any(label in blocked for label in labels)


def evaluate(
    profile: ProfileGate,
    *,
    input_name: str,
    domain: str,
    vendor_name: str = "",
    title: str = "",
    phone: str = "",
    address_state: str = "",
) -> GateResult:
    host = extract_domain(domain)
    if not host:
        return GateResult(False, reason="empty_domain")
    if is_blocklisted(host, profile.blocklist):
        return GateResult(False, domain=host, reason="blocklist")

    tld = tld_of(host)
    allowed = {t.lower() if t.startswith(".") else f".{t.lower()}" for t in profile.tld_allow}
    if tld not in allowed:
        return GateResult(False, domain=host, reason="tld")

    tokens = distinctive_tokens(input_name, profile.name_strip_tokens)
    hay = _blob(host, title, vendor_name).lower()
    token_hit = any(tok in hay for tok in tokens) if tokens else True
    token_on_vendor = any(tok in (vendor_name or "").lower() for tok in tokens) if tokens else False
    if tokens and not token_hit:
        return GateResult(False, domain=host, reason="token")

    industry_text = _blob(host, vendor_name, title)
    reject = _compile(profile.industry_reject_regex)
    if reject and reject.search(industry_text):
        return GateResult(False, domain=host, reason="industry_reject")
    allow = _compile(profile.industry_allow_regex)
    if allow and not allow.search(industry_text):
        return GateResult(False, domain=host, reason="industry_allow")

    geo_signal = False
    geo_ok = False
    if profile.area_codes and phone:
        geo_signal = True
        ac = area_code(phone)
        geo_ok = ac in {str(c).strip() for c in profile.area_codes}
        if profile.geo_required and not geo_ok:
            return GateResult(False, domain=host, reason="geo_area_code")
    if profile.states and address_state:
        geo_signal = True
        want = {normalize_state(s) for s in profile.states if s}
        got = normalize_state(address_state)
        state_ok = got in want or any(got.startswith(s) or s.startswith(got) for s in want if s)
        if geo_ok or not phone:
            geo_ok = state_ok if not (profile.area_codes and phone) else (geo_ok or state_ok)
        if profile.geo_required and profile.states and address_state and not state_ok:
            if not (profile.area_codes and phone and geo_ok):
                return GateResult(False, domain=host, reason="geo_state")
        if state_ok:
            geo_ok = True

    if not geo_signal:
        # No phone/address came back — national profiles cap, required profiles reject? Spec:
        # "If neither signal came back, cap at 0.7."
        geo_ok = False

    if token_on_vendor and geo_ok:
        confidence = 0.9
        status = "resolved"
    elif token_on_vendor or geo_ok:
        confidence = 0.7
        status = "resolved"
    else:
        confidence = 0.5
        status = "review"

    if not profile.geo_required and not geo_ok:
        confidence = min(confidence, 0.7)
        if confidence < 0.7 and status == "resolved":
            status = "review"

    return GateResult(
        accepted=True,
        domain=host,
        reason="ok",
        token_hit=token_hit,
        token_on_vendor_name=token_on_vendor,
        geo_ok=geo_ok,
        geo_signal=geo_signal,
        confidence=confidence,
        status=status,
    )


def find_sinks(domain_to_names: dict[str, set[str]], threshold: int = SINK_THRESHOLD) -> set[str]:
    return {d for d, names in domain_to_names.items() if len(names) > threshold}


def sink_map(rows: list[dict[str, Any]]) -> set[str]:
    claimed: dict[str, set[str]] = {}
    for row in rows:
        domain = extract_domain(str(row.get("domain") or ""))
        name = str(row.get("company_name") or row.get("input_name") or "").strip().lower()
        if not domain or not name:
            continue
        claimed.setdefault(domain, set()).add(name)
    return find_sinks(claimed)
