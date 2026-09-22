from domain_waterfall.normalize import (
    area_code,
    distinctive_tokens,
    domain_name_part,
    domain_tokens,
    extract_domain,
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
