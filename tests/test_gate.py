from domain_waterfall.gate import ProfileGate, evaluate, find_sinks, is_blocklisted


def _peterson() -> ProfileGate:
    return ProfileGate(
        name_strip_tokens=["inc", "llc", "construction", "contractors", "builders", "group"],
        industry_reject_regex=r"roof|hvac|plumb|electric|paint|storage|interior|design|landscap|fence|pool|sign|concrete|glass",
        area_codes=["214", "469", "972"],
        states=["TX"],
        geo_required=True,
        tld_allow=[".com", ".net", ".org", ".us", ".co", ".io", ".biz", ".build"],
    )


def test_blocklist_and_gov() -> None:
    assert is_blocklisted("yelp.com", {"yelp"})
    assert is_blocklisted("dallas.gov", set())


def test_token_and_industry_reject() -> None:
    gate = _peterson()
    bad = evaluate(
        gate,
        input_name="Summit Builders LLC",
        domain="summitroofing.com",
        vendor_name="Summit Roofing",
        phone="2145551212",
        address_state="TX",
    )
    assert not bad.accepted
    assert bad.reason == "industry_reject"

    ok = evaluate(
        gate,
        input_name="Summit Builders LLC",
        domain="summitbuilders.com",
        vendor_name="Summit Builders",
        phone="2145551212",
        address_state="TX",
    )
    assert ok.accepted
    assert ok.confidence == 0.9
    assert ok.status == "resolved"


def test_geo_required_rejects_wrong_area_code() -> None:
    gate = _peterson()
    out = evaluate(
        gate,
        input_name="Summit Builders",
        domain="summitbuilders.com",
        vendor_name="Summit Builders",
        phone="2125551212",
        address_state="NY",
    )
    assert not out.accepted
    assert out.reason in {"geo_area_code", "geo_state"}


def test_national_geo_caps_confidence() -> None:
    gate = ProfileGate(
        name_strip_tokens=["inc"],
        geo_required=False,
        states=[],
        area_codes=[],
    )
    out = evaluate(
        gate,
        input_name="Acme Widgets Inc",
        domain="acmewidgets.com",
        vendor_name="Other Corp",
    )
    assert out.accepted
    assert out.confidence <= 0.7


def test_industry_allow() -> None:
    gate = ProfileGate(
        name_strip_tokens=["inc"],
        industry_allow_regex=r"dealer|auto|automotive",
        geo_required=False,
    )
    miss = evaluate(gate, input_name="Smith Inc", domain="smithplumbing.com", vendor_name="Smith Plumbing")
    assert not miss.accepted
    hit = evaluate(gate, input_name="Smith Inc", domain="smithauto.com", vendor_name="Smith Auto Dealer")
    assert hit.accepted


def test_sinks() -> None:
    sinks = find_sinks(
        {
            "buildzoom.com": {"a", "b", "c", "d"},
            "good.com": {"a"},
        }
    )
    assert "buildzoom.com" in sinks
    assert "good.com" not in sinks
