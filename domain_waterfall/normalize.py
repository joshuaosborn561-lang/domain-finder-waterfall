"""Name and domain normalization. Industry words live in the profile, not here."""

from __future__ import annotations

import re
from urllib.parse import urlparse

_PUNCT = re.compile(r"[^a-z0-9]+")
_WWW = re.compile(r"^www\.", re.I)


def normalize_name(value: str) -> str:
    text = (value or "").lower().strip()
    text = _PUNCT.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()


def strip_tokens(name: str, tokens: list[str]) -> str:
    words = normalize_name(name).split()
    drop = {normalize_name(t) for t in tokens if t}
    kept = [w for w in words if w not in drop and len(w) > 1]
    return " ".join(kept)


def distinctive_tokens(name: str, tokens: list[str]) -> list[str]:
    stripped = strip_tokens(name, tokens)
    return [w for w in stripped.split() if len(w) >= 3]


def domain_name_part(domain: str) -> str:
    """Registrable label without TLD, e.g. seniorcareauthority.com → seniorcareauthority."""
    host = registrable(domain) or extract_domain(domain)
    if "." in host:
        return host.rsplit(".", 1)[0]
    return host


def domain_tokens(domain: str) -> list[str]:
    part = domain_name_part(domain)
    return [w for w in _PUNCT.split(part) if len(w) >= 3]


def extract_domain(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    try:
        host = urlparse(raw).hostname or ""
    except ValueError:
        host = ""
    host = _WWW.sub("", host).rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def registrable(domain: str) -> str:
    host = extract_domain(domain)
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) >= 2:
        return ".".join(parts[-2:])
    return host


def tld_of(domain: str) -> str:
    host = extract_domain(domain)
    if "." not in host:
        return ""
    return "." + host.rsplit(".", 1)[-1]


def area_code(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if digits.startswith("1") and len(digits) == 11:
        digits = digits[1:]
    if len(digits) >= 10:
        return digits[:3]
    return ""


def e164_us(phone: str) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if digits.startswith("1") and len(digits) == 11:
        return "+" + digits
    if len(digits) == 10:
        return "+1" + digits
    return (phone or "").strip()


def normalize_state(value: str) -> str:
    return re.sub(r"[^a-z]", "", (value or "").lower())
