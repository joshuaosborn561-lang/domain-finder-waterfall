from domain_waterfall.vendors import cache
from domain_waterfall.vendors.base import DomainCandidate, TierResult


def test_cache_hits_identical_with_and_without_city(monkeypatch) -> None:
    def fake_lookup(names, extra_tables=None):
        result = TierResult(tier="cache", inputs_passed=["company_name_normalized"])
        result.candidates["Acme Builders LLC"] = DomainCandidate(
            domain="acmebuilders.com",
            inputs_passed=["company_name_normalized"],
        )
        return result

    monkeypatch.setattr(cache, "lookup_cache", fake_lookup)
    with_city = cache.lookup_many(
        [{"_source_key": 1, "company_name": "Acme Builders LLC", "city": "Dallas"}]
    )
    without_city = cache.lookup_many(
        [{"_source_key": 1, "company_name": "Acme Builders LLC", "city": ""}]
    )
    assert with_city.inputs_passed == ["company_name_normalized"]
    assert set(with_city.candidates) == set(without_city.candidates) == {"1"}
    assert with_city.candidates["1"].domain == without_city.candidates["1"].domain
