from domain_waterfall.profiles import ClientProfile, deep_merge
from domain_waterfall.seed_profiles import GOLIATH, PETERSON_ROOF


def test_profiles_differ_only_in_json() -> None:
    p = ClientProfile("peterson_roof", "Peterson", PETERSON_ROOF)
    g = ClientProfile("goliath", "Goliath", GOLIATH)
    assert p.geo_required is True
    assert g.geo_required is False
    assert p.industry_reject_regex
    assert g.industry_reject_regex is None
    assert p.states == ["TX"]
    assert g.states == []
    assert p.area_codes
    assert g.area_codes == []
    assert ".build" in p.tld_allow
    assert ".build" not in g.tld_allow
    assert p.cache_tables == ["gc.companies"]
    assert g.cache_tables == ["public.goliath_wf_companies"]


def test_deep_merge_keeps_people_keys() -> None:
    existing = {
        "geo": {"states": ["TX"], "person_geo_mode": "cap"},
        "target_titles": ["Owner"],
        "ground_truth": {"contacts_table": "gc.contacts"},
        "cache_tables": ["gc.contacts"],
    }
    merged = deep_merge(existing, PETERSON_ROOF)
    assert merged["target_titles"] == ["Owner"]
    assert merged["cache_tables"] == ["gc.contacts"]
    assert merged["domain_cache_tables"] == ["gc.companies"]
    assert merged["geo"]["person_geo_mode"] == "cap"
    assert merged["geo"]["geo_required"] is True
    assert merged["geo"]["area_codes"]
    assert merged["ground_truth"]["contacts_table"] == "gc.contacts"
    assert merged["ground_truth"]["table"] == "client_peterson.gc_adjudication"
