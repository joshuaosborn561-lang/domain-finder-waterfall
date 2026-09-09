"""Local runner for receipt + resolve. Counts only — never prints row payloads."""

from __future__ import annotations

import argparse
import json
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="domain-waterfall")
    sub = parser.add_subparsers(dest="cmd", required=True)

    rec = sub.add_parser("receipt", help="Score tiers against profile ground truth")
    rec.add_argument("client_tag")
    rec.add_argument("-n", type=int, default=25)
    rec.add_argument("--no-persist", action="store_true")

    res = sub.add_parser("resolve", help="Resolve domains onto a source table")
    res.add_argument("source_table")
    res.add_argument("where")
    res.add_argument("client_tag")
    res.add_argument("--max-tier", default="")
    res.add_argument("--approve-cost-usd", type=float, default=None)
    res.add_argument("--estimate-only", action="store_true")
    res.add_argument("--limit", type=int, default=None)
    res.add_argument("--no-writeback", action="store_true")

    seed = sub.add_parser("seed-profiles", help="Merge Peterson + Goliath domain fields")

    args = parser.parse_args(argv)

    def _print(data: object) -> None:
        print(json.dumps(data, indent=2, default=str))

    if args.cmd == "seed-profiles":
        from .profiles import ensure_profile
        from .seed_profiles import GOLIATH, PETERSON_ROOF

        peterson = ensure_profile("peterson_roof", PETERSON_ROOF)
        goliath = ensure_profile("goliath", GOLIATH)
        _print(
            {
                "ok": True,
                "seeded": [peterson.client_tag, goliath.client_tag],
                "peterson_geo_required": peterson.geo_required,
                "goliath_geo_required": goliath.geo_required,
                "peterson_industry_reject": bool(peterson.industry_reject_regex),
                "goliath_industry_reject": bool(goliath.industry_reject_regex),
            }
        )
        return 0

    if args.cmd == "receipt":
        from .receipt import receipt_test

        _print(receipt_test(args.client_tag, n=args.n, persist=not args.no_persist))
        return 0

    from .waterfall import resolve_domain

    _print(
        resolve_domain(
            source_table=args.source_table,
            where=args.where,
            client_tag=args.client_tag,
            max_tier=args.max_tier,
            approve_cost_usd=args.approve_cost_usd,
            estimate_only=args.estimate_only,
            writeback=not args.no_writeback,
            limit=args.limit,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
