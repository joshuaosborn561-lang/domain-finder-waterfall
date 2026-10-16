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
    from domain_waterfall.gate import GLOBAL_BLOCKLIST

    assert is_blocklisted("yelp.com", {"yelp"})
    assert is_blocklisted("dallas.gov", set())
    assert is_blocklisted("waze.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("foo.cybo.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("seniorcare.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("seniorcareauthority.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("localbiznetwork.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("charitywater.org", GLOBAL_BLOCKLIST)
    assert is_blocklisted("eacsociety.org", GLOBAL_BLOCKLIST)
    assert is_blocklisted("yellowpages.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("facebook.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("mapquest.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("aplaceformom.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("healthgrades.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("zocdoc.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("www.niche.com", GLOBAL_BLOCKLIST)
    assert is_blocklisted("greatschools.org", GLOBAL_BLOCKLIST)
    assert is_blocklisted("caring.com", GLOBAL_BLOCKLIST)
    assert not is_blocklisted("summitbuilders.com", GLOBAL_BLOCKLIST)


def test_title_match_does_not_accept_directory() -> None:
    gate = ProfileGate(name_strip_tokens=["inc", "llc"], geo_required=False)
    out = evaluate(
        gate,
        input_name="Oakridge Senior Living LLC",
        domain="waze.com",
        vendor_name="",
        title="Oakridge Senior Living, Sacramento CA, Waze",
    )
    assert not out.accepted
    assert out.reason in {"blocklist", "token"}


def test_domain_tokens_score_up_and_none_share_rejected() -> None:
    gate = ProfileGate(name_strip_tokens=["inc", "llc"], geo_required=False)
    hit = evaluate(
        gate,
        input_name="Oakridge Senior Living LLC",
        domain="oakridgeseniorliving.com",
        vendor_name="",
        title="Oakridge Senior Living",
    )
    assert hit.accepted
    assert hit.token_hit
    assert hit.confidence >= 0.7

    miss = evaluate(
        gate,
        input_name="Oakridge Senior Living LLC",
        domain="unrelatedwidgetsfactory.com",
        vendor_name="",
        title="Oakridge Senior Living, a local machine shop",
    )
    assert not miss.accepted
    assert miss.reason == "token"

    listed = evaluate(
        gate,
        input_name="Oakridge Senior Living LLC",
        domain="machinist.com",
        vendor_name="",
        title="Oakridge Senior Living, a local machine shop",
    )
    assert not listed.accepted
    assert listed.reason == "blocklist"


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


def _contractor(states: list[str] | None = None) -> ProfileGate:
    return ProfileGate(
        name_strip_tokens=[
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
        ],
        states=list(states or []),
        geo_required=True,
        tld_allow=[".com", ".net", ".org", ".us", ".co", ".io", ".biz"],
    )


def test_initials_and_title_prather() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Prather & Harlan",
        domain="ph-ac.com",
        title="Prather & Harlan Heating and Air",
        input_state="TX",
    )
    assert out.accepted
    assert out.accepted_by_title
    assert out.initials_match
    assert out.title_match
    assert not out.token_hit
    assert out.status == "review"
    assert out.confidence == 0.6


def test_initials_and_title_farrell() -> None:
    gate = _contractor(["FL"])
    out = evaluate(
        gate,
        input_name="John Farrell Construction",
        domain="jfconstfl.com",
        title="John Farrell Construction, Tampa",
        input_state="FL",
        city="Tampa",
    )
    assert out.accepted
    assert out.accepted_by_title
    assert out.initials_match
    assert out.title_match
    assert not out.token_hit
    assert out.status == "review"
    assert out.confidence == 0.6


def test_token_path_gill_unchanged() -> None:
    gate = _contractor(["PA"])
    out = evaluate(
        gate,
        input_name="Gill Construction",
        domain="gillconstructioninc.com",
        title="Anything at all",
        input_state="PA",
    )
    assert out.accepted
    assert out.token_hit
    assert not out.accepted_by_title
    assert out.status == "resolved"
    assert out.confidence >= 0.7


def test_blocklist_smith_yelp() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Smith Roofing",
        domain="yelp.com",
        title="Smith Roofing, Yelp",
        input_state="TX",
    )
    assert not out.accepted
    assert out.reason == "blocklist"


def test_title_alone_does_not_accept_unrelated_domain() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Smith Roofing",
        domain="acmeplumbing.com",
        title="Acme Plumbing Dallas",
        input_state="TX",
    )
    assert not out.accepted
    assert out.reason == "token"


def test_token_path_geo_state_from_title() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Smith Roofing",
        domain="smithroofingco.com",
        title="Smith Roofing Phoenix AZ",
        input_state="TX",
    )
    assert not out.accepted
    assert out.reason == "geo_state"


def test_initials_plus_title_geo_ok_resolves() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Prather & Harlan",
        domain="ph-ac.com",
        title="Prather & Harlan Heating and Air TX",
        input_state="TX",
    )
    assert out.accepted
    assert out.accepted_by_title
    assert out.status == "resolved"
    assert out.confidence == 0.8


def test_maps_listing_match_resolves() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Prather & Harlan Heating & Air",
        domain="ph-ac.com",
        vendor_name="Prather & Harlan Heating & Air",
        address_state="TX",
        input_state="TX",
        source="maps",
    )
    assert out.accepted
    assert out.accepted_by_listing
    assert not out.listing_review
    assert not out.accepted_by_title
    assert not out.token_hit
    assert out.status == "resolved"
    assert out.confidence == 0.85


def test_maps_listing_different_business_token() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Prather & Harlan Heating & Air",
        domain="ph-ac.com",
        vendor_name="Acme Plumbing Dallas",
        address_state="TX",
        input_state="TX",
        source="maps",
    )
    assert not out.accepted
    assert out.reason == "token"
    assert not out.accepted_by_listing
    assert not out.listing_review


def test_maps_listing_state_mismatch_geo_state() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Prather & Harlan Heating & Air",
        domain="ph-ac.com",
        vendor_name="Prather & Harlan Heating & Air",
        address_state="FL",
        input_state="TX",
        source="maps",
    )
    assert not out.accepted
    assert out.reason == "geo_state"
    assert not out.accepted_by_listing
    assert not out.listing_review


def test_maps_listing_phone_overrides_state_mismatch() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Prather & Harlan Heating & Air",
        domain="ph-ac.com",
        vendor_name="Prather & Harlan Heating & Air",
        address_state="FL",
        input_state="TX",
        phone="214-555-1212",
        input_phone="(214) 555-1212",
        source="maps",
    )
    assert out.accepted
    assert out.accepted_by_listing
    assert not out.listing_review
    assert out.status == "resolved"
    assert out.confidence == 0.85


def test_maps_listing_area_code_does_not_override_state() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Prather & Harlan Heating & Air",
        domain="ph-ac.com",
        vendor_name="Prather & Harlan Heating & Air",
        address_state="FL",
        input_state="TX",
        phone="214-555-9999",
        input_phone="214-555-1212",
        source="maps",
    )
    assert not out.accepted
    assert out.reason == "geo_state"
    assert not out.accepted_by_listing
    assert not out.listing_review


def test_maps_listing_skips_industry_filter() -> None:
    gate = _peterson()
    out = evaluate(
        gate,
        input_name="Summit Roofing",
        domain="sr-tx.com",
        vendor_name="Summit Roofing",
        address_state="TX",
        input_state="TX",
        phone="2145551212",
        input_phone="2145551212",
        source="maps",
    )
    assert out.accepted
    assert out.accepted_by_listing
    assert out.status == "resolved"
    assert out.confidence == 0.85


def test_maps_listing_no_location_is_review() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Prather & Harlan Heating & Air",
        domain="ph-ac.com",
        vendor_name="Prather & Harlan Heating & Air",
        source="maps",
    )
    assert out.accepted
    assert out.listing_review
    assert not out.accepted_by_listing
    assert out.status == "review"
    assert out.confidence == 0.6


def test_serp_no_domain_token_unchanged() -> None:
    gate = _contractor(["TX"])
    out = evaluate(
        gate,
        input_name="Prather & Harlan",
        domain="ph-ac.com",
        title="Prather & Harlan Heating and Air",
        input_state="TX",
        source="serp",
    )
    assert out.accepted
    assert out.accepted_by_title
    assert not out.accepted_by_listing
    assert not out.listing_review
    assert out.initials_match
    assert out.title_match
    assert not out.token_hit
    assert out.status == "review"
    assert out.confidence == 0.6


def test_sinks() -> None:
    sinks = find_sinks(
        {
            "buildzoom.com": {"a", "b", "c", "d"},
            "good.com": {"a"},
            "almost.com": {"one", "two", "three"},
        }
    )
    assert "buildzoom.com" in sinks
    assert "good.com" not in sinks
    # More than 3 unrelated companies in one run. Three names stay.
    assert "almost.com" not in sinks
