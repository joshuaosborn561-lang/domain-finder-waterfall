"""Environment-backed settings. Vendor keys may also come from private.api_keys."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

DEFAULT_SUPABASE_URL = "https://azpapwtnrbzywlnxxecz.supabase.co"
DEFAULT_SUPABASE_PROJECT = "azpapwtnrbzywlnxxecz"

API_KEY_ALIASES: dict[str, tuple[str, ...]] = {
    "discolike": ("DISCOLIKE_API_KEY", "DISCOLIKE_KEY"),
    "aiark": ("AI_ARK_API_KEY", "AIARK_API_KEY"),
    "leadmagic": ("LEADMAGIC_API_KEY", "LEADMAGIC_KEY"),
    "prospeo": ("PROSPEO_API_KEY", "PROSPEO_KEY"),
    "apify": ("APIFY_TOKEN", "APIFY_API_TOKEN"),
    "rapidapi": ("RAPIDAPI_KEY", "MAPS_RAPIDAPI_KEY"),
}


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _first_env(*names: str) -> str:
    for name in names:
        val = _env(name)
        if val:
            return val
    return ""


@dataclass
class Settings:
    supabase_url: str
    supabase_service_role_key: str
    supabase_anon_key: str
    discolike_api_key: str
    ai_ark_api_key: str
    leadmagic_api_key: str
    prospeo_api_key: str
    apify_token: str
    apify_actor: str
    rapidapi_key: str
    maps_host: str
    extra_keys: dict[str, str]

    @property
    def supabase_key(self) -> str:
        return self.supabase_service_role_key or self.supabase_anon_key

    @property
    def supabase_configured(self) -> bool:
        return bool(self.supabase_url and self.supabase_key)

    def vendor_key(self, name: str) -> str:
        mapping = {
            "discolike": self.discolike_api_key,
            "aiark": self.ai_ark_api_key,
            "leadmagic": self.leadmagic_api_key,
            "prospeo": self.prospeo_api_key,
            "apify": self.apify_token,
            "serp": self.apify_token,
            "rapidapi": self.rapidapi_key,
            "maps": self.rapidapi_key,
        }
        return (mapping.get(name) or self.extra_keys.get(name) or "").strip()


def load_settings() -> Settings:
    return Settings(
        supabase_url=_env("SUPABASE_URL", DEFAULT_SUPABASE_URL).rstrip("/"),
        supabase_service_role_key=_env("SUPABASE_SERVICE_ROLE_KEY"),
        supabase_anon_key=_env("SUPABASE_ANON_KEY"),
        discolike_api_key=_first_env(*API_KEY_ALIASES["discolike"]),
        ai_ark_api_key=_first_env(*API_KEY_ALIASES["aiark"]),
        leadmagic_api_key=_first_env(*API_KEY_ALIASES["leadmagic"]),
        prospeo_api_key=_first_env(*API_KEY_ALIASES["prospeo"]),
        apify_token=_first_env(*API_KEY_ALIASES["apify"]),
        apify_actor=_env("APIFY_GOOGLE_SEARCH_ACTOR", "apify/google-search-scraper"),
        rapidapi_key=_first_env(*API_KEY_ALIASES["rapidapi"]),
        maps_host=_env("MAPS_DATA_HOST", "maps-data.p.rapidapi.com"),
        extra_keys={},
    )


settings = load_settings()
