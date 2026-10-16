"""Acceptance gate. Every vendor output passes this or does not count."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable

from .normalize import (
    area_code,
    distinctive_tokens,
    domain_name_part,
    extract_domain,
    normalize_state,
    phone_digits,
    tld_of,
)

# Exact labels plus prefix patterns ending in *. Applied before scoring.
GLOBAL_BLOCKLIST = frozenset(
    {
        "yelp",
        "bbb",
        "buildzoom",
        "houzz",
        "facebook",
        "linkedin",
        "instagram",
        "twitter",
        "tiktok",
        "youtube",
        "indeed",
        "zoominfo",
        "manta",
        "mapquest",
        "waze",
        "yellowpages",
        "superpages",
        "whitepages",
        "cybo",
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
        "chamberofcommerce",
        "localbiznetwork",
        "seniorcare*",
        "caring",
        "aplaceformom",
        "healthgrades",
        "zocdoc",
        "niche",
        "greatschools",
        "charitywater",
        "charitynavigator",
        "eacsociety",
        "gofundme",
        "justgiving",
        "tripadvisor",
        "nextdoor",
        "hotfrog",
        "citysearch",
        "foursquare",
        "machinist",
    }
)

DEFAULT_TLDS = (".com", ".net", ".org", ".us", ".co", ".io", ".biz")
SINK_THRESHOLD = 3

# Used only for the one-token title path (token + trade word or city).
TRADE_WORDS = frozenset(
    {
        "heating",
        "air",
        "hvac",
        "plumbing",
        "plumb",
        "roofing",
        "roof",
        "electric",
        "electrical",
        "construction",
        "contractor",
        "contractors",
        "builder",
        "builders",
        "painting",
        "paint",
        "concrete",
        "fencing",
        "fence",
        "landscape",
        "landscaping",
        "excavating",
        "excavation",
        "remodeling",
        "remodel",
        "restoration",
        "insulation",
        "flooring",
        "masonry",
        "carpentry",
        "welding",
        "paving",
        "siding",
        "drywall",
        "mechanical",
        "cooling",
        "refrigeration",
    }
)

_US_STATE_ABBR = frozenset(
    {
        "al",
        "ak",
        "az",
        "ar",
        "ca",
        "co",
        "ct",
        "de",
        "dc",
        "fl",
        "ga",
        "hi",
        "id",
        "il",
        "in",
        "ia",
        "ks",
        "ky",
        "la",
        "me",
        "md",
        "ma",
        "mi",
        "mn",
        "ms",
        "mo",
        "mt",
        "ne",
        "nv",
        "nh",
        "nj",
        "nm",
        "ny",
        "nc",
        "nd",
        "oh",
        "ok",
        "or",
        "pa",
        "ri",
        "sc",
        "sd",
        "tn",
        "tx",
        "ut",
        "vt",
        "va",
        "wa",
        "wv",
        "wi",
        "wy",
    }
)

# Full names → USPS so a Maps listing "Texas" matches a row state "TX".
_US_STATE_NAMES = {
    "alabama": "al",
    "alaska": "ak",
    "arizona": "az",
    "arkansas": "ar",
    "california": "ca",
    "colorado": "co",
    "connecticut": "ct",
    "delaware": "de",
    "district of columbia": "dc",
    "florida": "fl",
    "georgia": "ga",
    "hawaii": "hi",
    "idaho": "id",
    "illinois": "il",
    "indiana": "in",
    "iowa": "ia",
    "kansas": "ks",
    "kentucky": "ky",
    "louisiana": "la",
    "maine": "me",
    "maryland": "md",
    "massachusetts": "ma",
    "michigan": "mi",
    "minnesota": "mn",
    "mississippi": "ms",
    "missouri": "mo",
    "montana": "mt",
    "nebraska": "ne",
    "nevada": "nv",
    "new hampshire": "nh",
    "new jersey": "nj",
    "new mexico": "nm",
    "new york": "ny",
    "north carolina": "nc",
    "north dakota": "nd",
    "ohio": "oh",
    "oklahoma": "ok",
    "oregon": "or",
    "pennsylvania": "pa",
    "rhode island": "ri",
    "south carolina": "sc",
    "south dakota": "sd",
    "tennessee": "tn",
    "texas": "tx",
    "utah": "ut",
    "vermont": "vt",
    "virginia": "va",
    "washington": "wa",
    "west virginia": "wv",
    "wisconsin": "wi",
    "wyoming": "wy",
}


def _state_key(value: str) -> str:
    raw = normalize_state(value)
    if not raw:
        return ""
    if raw in _US_STATE_ABBR:
        return raw
    return _US_STATE_NAMES.get(raw, raw)


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
    accepted_by_title: bool = False
    accepted_by_listing: bool = False
    title_match: bool = False
    initials_match: bool = False


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
    min_confidence: float = 0.25

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


def _block_patterns(blocklist: Iterable[str]) -> tuple[set[str], list[str]]:
    exact: set[str] = set()
    prefixes: list[str] = []
    for raw in blocklist:
        item = str(raw or "").strip().lower().lstrip(".")
        if not item:
            continue
        if item.endswith("*"):
            prefixes.append(item[:-1])
        else:
            exact.add(item)
    return exact, prefixes


def _matches_block(label: str, exact: set[str], prefixes: list[str]) -> bool:
    if label in exact:
        return True
    return any(label.startswith(p) for p in prefixes if p)


def _whole_word(token: str, text: str) -> bool:
    if not token or not text:
        return False
    return bool(re.search(rf"(?<![a-z0-9]){re.escape(token)}(?![a-z0-9])", text, re.I))


def title_has_all_tokens(title: str, tokens: list[str]) -> bool:
    """True when the title contains every distinctive token as a whole word."""
    if not tokens:
        return False
    text = title or ""
    return all(_whole_word(tok, text) for tok in tokens)


def domain_starts_with_initials(name_part: str, tokens: list[str]) -> bool:
    """True when the registrable label starts with the first two token initials."""
    if len(tokens) < 2:
        return False
    initials = f"{tokens[0][0]}{tokens[1][0]}".lower()
    raw = (name_part or "").lower()
    compact = re.sub(r"[^a-z0-9]", "", raw)
    return raw.startswith(initials) or compact.startswith(initials)


def _state_from_title(title: str) -> str:
    """Last uppercase 2-letter USPS code in the title. Ignores lowercase 'in'/'or'."""
    found = [
        tok.lower()
        for tok in re.findall(r"\b[A-Z]{2}\b", title or "")
        if tok.lower() in _US_STATE_ABBR
    ]
    return found[-1] if found else ""


def listing_name_match(
    vendor_name: str,
    tokens: list[str],
    *,
    city: str = "",
    input_name: str = "",
    strip_tokens: list[str] | None = None,
) -> bool:
    """Every distinctive token appears in the Maps listing name (whole word)."""
    if not title_has_all_tokens(vendor_name, tokens):
        return False
    if len(tokens) == 1:
        return _one_token_title_extra(
            vendor_name, city, input_name, list(strip_tokens or [])
        )
    return True


def listing_location(
    *,
    address_state: str = "",
    input_state: str = "",
    listing_phone: str = "",
    input_phone: str = "",
) -> tuple[bool, bool]:
    """Return (has_location_signal, location_disagrees).

    Signal: listing state equals row state, listing area code is one the row
    phone uses, or the two phones match after digit normalization.
    Disagreement: both sides have a state and they are not the same.
    """
    row_st = _state_key(input_state)
    list_st = _state_key(address_state)
    disagree = bool(row_st and list_st and row_st != list_st)
    signal = bool(row_st and list_st and row_st == list_st)

    row_digits = phone_digits(input_phone)
    list_digits = phone_digits(listing_phone)
    if row_digits and list_digits and row_digits == list_digits:
        signal = True
    row_ac = area_code(input_phone)
    list_ac = area_code(listing_phone)
    if row_ac and list_ac and row_ac == list_ac:
        signal = True
    return signal, disagree


def _one_token_title_extra(
    title: str,
    city: str,
    input_name: str,
    strip_tokens: list[str],
) -> bool:
    if city and _whole_word(city.strip(), title or ""):
        return True
    extras = set(TRADE_WORDS)
    name_norm = (input_name or "").lower()
    for raw in strip_tokens:
        tok = str(raw or "").strip().lower()
        if tok and tok in name_norm:
            extras.add(tok)
    return any(_whole_word(word, title or "") for word in extras if word)


def is_blocklisted(domain: str, blocklist: Iterable[str]) -> bool:
    host = extract_domain(domain)
    if not host:
        return True
    if host.endswith(".gov") or host.endswith(".gov.uk"):
        return True
    exact, prefixes = _block_patterns(blocklist)
    if any(_matches_block(label, exact, prefixes) for label in host.split(".") if label):
        return True
    return _matches_block(domain_name_part(host), exact, prefixes)


def evaluate(
    profile: ProfileGate,
    *,
    input_name: str,
    domain: str,
    vendor_name: str = "",
    title: str = "",
    phone: str = "",
    address_state: str = "",
    city: str = "",
    input_state: str = "",
    source: str = "",
    input_phone: str = "",
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
    name_part = domain_name_part(host)
    token_on_domain = any(tok in name_part for tok in tokens) if tokens else False
    token_on_vendor = any(tok in (vendor_name or "").lower() for tok in tokens) if tokens else False
    token_hit = token_on_domain
    title_match = title_has_all_tokens(title, tokens)
    initials_only = domain_starts_with_initials(name_part, tokens)
    initials_match = bool(initials_only and title_match)
    one_token_ok = bool(
        len(tokens) == 1
        and title_match
        and _one_token_title_extra(title, city, input_name, profile.name_strip_tokens)
    )
    second_path = bool(tokens and not token_on_domain and (initials_match or one_token_ok))
    listing_path = bool(
        tokens
        and not token_on_domain
        and (source or "").strip().lower() == "maps"
        and listing_name_match(
            vendor_name,
            tokens,
            city=city,
            input_name=input_name,
            strip_tokens=profile.name_strip_tokens,
        )
    )
    if tokens and not token_on_domain and not second_path and not listing_path:
        return GateResult(
            False,
            domain=host,
            reason="token",
            title_match=title_match,
            initials_match=initials_match,
        )

    industry_text = _blob(host, vendor_name, title)
    reject = _compile(profile.industry_reject_regex)
    if reject and reject.search(industry_text):
        return GateResult(False, domain=host, reason="industry_reject")
    allow = _compile(profile.industry_allow_regex)
    if allow and not allow.search(industry_text):
        return GateResult(False, domain=host, reason="industry_allow")

    if listing_path:
        loc_ok, loc_bad = listing_location(
            address_state=address_state,
            input_state=input_state,
            listing_phone=phone,
            input_phone=input_phone,
        )
        if loc_bad:
            return GateResult(
                False,
                domain=host,
                reason="geo_state",
                title_match=title_match,
                initials_match=initials_match,
            )
        if loc_ok:
            confidence = 0.85
            status = "resolved"
        else:
            confidence = 0.6
            status = "review"
        return GateResult(
            accepted=True,
            domain=host,
            reason="ok",
            token_hit=False,
            token_on_vendor_name=token_on_vendor,
            geo_ok=loc_ok,
            geo_signal=loc_ok,
            confidence=confidence,
            status=status,
            accepted_by_listing=True,
            title_match=title_match,
            initials_match=initials_match,
        )

    if not address_state:
        address_state = _state_from_title(title)

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
    if input_state and address_state:
        geo_signal = True
        if normalize_state(input_state) != normalize_state(address_state):
            return GateResult(False, domain=host, reason="geo_state")
        geo_ok = True

    if not geo_signal:
        # No phone/address came back — national profiles cap, required profiles reject? Spec:
        # "If neither signal came back, cap at 0.7."
        geo_ok = False

    if second_path:
        if geo_ok:
            confidence = 0.8
            status = "resolved"
        else:
            confidence = 0.6
            status = "review"
        return GateResult(
            accepted=True,
            domain=host,
            reason="ok",
            token_hit=False,
            token_on_vendor_name=token_on_vendor,
            geo_ok=geo_ok,
            geo_signal=geo_signal,
            confidence=confidence,
            status=status,
            accepted_by_title=True,
            title_match=title_match,
            initials_match=initials_match,
        )

    if token_on_domain and (token_on_vendor or geo_ok):
        confidence = 0.9
        status = "resolved"
    elif token_on_vendor and geo_ok:
        confidence = 0.9
        status = "resolved"
    elif token_on_domain:
        # Domain tokens match the company name: score up from the old 0.5 title-only path.
        confidence = 0.7
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
        title_match=title_match,
        initials_match=initials_match,
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
