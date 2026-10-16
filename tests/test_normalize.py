from domain_waterfall.normalize import (
    US_STATE_ABBR,
    area_code,
    distinctive_tokens,
    domain_name_part,
    domain_tokens,
    extract_domain,
    normalize_state,
    registrable,
    strip_tokens,
)


def test_domain_extract() -> None:
    assert extract_domain("https://www.SummitBuilders.com/about") == "summitbuilders.com"
    assert registrable("jobs.summitbuilders.com") == "summitbuilders.com"


def test_strip_and_tokens() -> None:
    tokens = distinctive_tokens(
        "Summit Builders LLC",
        ["inc", "llc", "builders", "construction"],
    )
    assert "summit" in tokens
    assert "builders" not in tokens
    assert strip_tokens("Acme Construction Inc", ["inc", "construction"]) == "acme"


def test_area_code() -> None:
    assert area_code("(214) 555-1212") == "214"
    assert area_code("+1 469 555 0100") == "469"


def test_domain_name_part_and_tokens() -> None:
    assert domain_name_part("seniorcareauthority.com") == "seniorcareauthority"
    assert domain_name_part("foo.cybo.com") == "cybo"
    assert "oakridge" in domain_tokens("oakridge-senior-living.com")


def test_normalize_state_florida_matches_fl() -> None:
    assert normalize_state("Florida") == normalize_state("FL") == "fl"
    assert normalize_state("florida") == "fl"
    assert normalize_state("Fl") == "fl"


def test_normalize_state_new_york_matches_ny() -> None:
    assert normalize_state("New York") == normalize_state("NY") == "ny"
    assert normalize_state("new york") == "ny"
    assert normalize_state("NEW YORK") == "ny"


def test_normalize_state_florida_vs_tx_mismatch() -> None:
    assert normalize_state("Florida") != normalize_state("TX")
    assert normalize_state("Florida") == "fl"
    assert normalize_state("TX") == "tx"


def test_normalize_state_all_names_and_territories() -> None:
    names = {
        "Alabama": "al",
        "Alaska": "ak",
        "Arizona": "az",
        "Arkansas": "ar",
        "California": "ca",
        "Colorado": "co",
        "Connecticut": "ct",
        "Delaware": "de",
        "District of Columbia": "dc",
        "Florida": "fl",
        "Georgia": "ga",
        "Hawaii": "hi",
        "Idaho": "id",
        "Illinois": "il",
        "Indiana": "in",
        "Iowa": "ia",
        "Kansas": "ks",
        "Kentucky": "ky",
        "Louisiana": "la",
        "Maine": "me",
        "Maryland": "md",
        "Massachusetts": "ma",
        "Michigan": "mi",
        "Minnesota": "mn",
        "Mississippi": "ms",
        "Missouri": "mo",
        "Montana": "mt",
        "Nebraska": "ne",
        "Nevada": "nv",
        "New Hampshire": "nh",
        "New Jersey": "nj",
        "New Mexico": "nm",
        "New York": "ny",
        "North Carolina": "nc",
        "North Dakota": "nd",
        "Ohio": "oh",
        "Oklahoma": "ok",
        "Oregon": "or",
        "Pennsylvania": "pa",
        "Puerto Rico": "pr",
        "Rhode Island": "ri",
        "South Carolina": "sc",
        "South Dakota": "sd",
        "Tennessee": "tn",
        "Texas": "tx",
        "Utah": "ut",
        "Vermont": "vt",
        "Virginia": "va",
        "Washington": "wa",
        "West Virginia": "wv",
        "Wisconsin": "wi",
        "Wyoming": "wy",
    }
    assert len(names) == 52
    for name, abbr in names.items():
        assert normalize_state(name) == abbr
        assert normalize_state(name.upper()) == abbr
        assert normalize_state(abbr) == abbr
        assert normalize_state(abbr.upper()) == abbr
        assert abbr in US_STATE_ABBR
    assert normalize_state("") == ""

