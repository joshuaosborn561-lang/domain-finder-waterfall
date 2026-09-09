from domain_waterfall.pricing import compute_order, estimate_rows, sort_cost


def test_free_first_then_unit_price() -> None:
    order = compute_order(
        ["leadmagic", "cache", "discolike", "aiark", "maps"],
        live_units={"aiark": 0.0004, "discolike": 0.00425, "leadmagic": 0.02},
    )
    assert order.tiers[0] == "cache"
    assert order.tiers[1] == "maps"
    assert order.tiers.index("aiark") < order.tiers.index("discolike")
    assert order.tiers.index("discolike") < order.tiers.index("leadmagic")


def test_free_on_miss_uses_hit_rate() -> None:
    # unit 0.02 * 0.1 hit rate = 0.002, cheaper than always-billed 0.00425
    cheap = sort_cost("prospeo", 0.02, {"prospeo": 0.1})
    assert cheap == 0.002
    default = sort_cost("prospeo", 0.02, {})
    assert default == 0.01  # half until measured


def test_zero_yield_dropped() -> None:
    order = compute_order(
        ["cache", "maps", "discolike"],
        dropped=["discolike"],
    )
    assert "discolike" not in order.tiers


def test_explicit_tier_order_is_frozen() -> None:
    order = compute_order(
        ["cache", "maps", "discolike", "prospeo", "aiark", "serp", "leadmagic"],
        live_units={"discolike": 0.00425, "prospeo": 0.015, "aiark": 0.0005, "serp": 0.0045},
        explicit_order=["maps", "discolike", "prospeo", "aiark", "serp", "cache", "leadmagic"],
    )
    assert order.tiers == [
        "maps",
        "discolike",
        "prospeo",
        "aiark",
        "serp",
        "cache",
        "leadmagic",
    ]


def test_estimate_free_zero() -> None:
    order = compute_order(["cache", "maps", "discolike"], live_units={"discolike": 0.00425})
    est = estimate_rows(25, order)
    assert est["tiers"][0]["estimated_usd"] == 0
    assert est["estimated_usd"] == round(25 * 0.00425, 4)
