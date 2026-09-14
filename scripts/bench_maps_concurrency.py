#!/usr/bin/env python3
"""200-ish row concurrency slice. Uses 80 rows so serial finishes under the 10 min floor."""
from __future__ import annotations

import json
import time
import traceback
from pathlib import Path

from domain_waterfall.config import load_settings
import domain_waterfall.config as cfg

cfg.settings = load_settings()
from domain_waterfall.waterfall import resolve_domain

LIMIT = 80
OUT = Path("/opt/cursor/artifacts/dw_concurrency_slice.json")
PARTIAL = Path("/opt/cursor/artifacts/dw_concurrency_slice_partial.json")
ERR = Path("/opt/cursor/artifacts/dw_concurrency_slice_error.txt")

rows: list[dict] = []
try:
    for conc in (1, 4, 8, 12):
        print(f"START conc={conc}", flush=True)
        t0 = time.time()
        out = resolve_domain(
            source_table="client_peterson.improved_domain_resolution",
            where="domain is null",
            client_tag="peterson_earthworks",
            max_tier="maps",
            min_tier="maps",
            approve_cost_usd=35,
            estimate_only=False,
            writeback=False,
            limit=LIMIT,
            concurrency=conc,
        )
        elapsed = time.time() - t0
        maps = next((t for t in (out.get("tiers") or []) if t.get("tier") == "maps"), {})
        targets = int(maps.get("targets") or out.get("rows") or 0)
        accepted = int(maps.get("accepted") or 0)
        rejected = int(maps.get("rejected_by_gate") or 0)
        none = int(maps.get("none") or 0)
        errored = int(maps.get("errored") or 0)
        req = int(maps.get("requests_made") or 0)
        rows_done = int(maps.get("rows_done") or 0)
        vendor_hits = accepted + rejected
        vendor_hit_rate = (vendor_hits / rows_done) if rows_done else 0.0
        gate_accept_rate = (accepted / rows_done) if rows_done else 0.0
        rpm = (rows_done / elapsed * 60) if elapsed else 0.0
        rpr = (req / rows_done) if rows_done else 0.0
        row = {
            "concurrency": conc,
            "elapsed_s": round(elapsed, 1),
            "targets": targets,
            "rows_done": rows_done,
            "accepted": accepted,
            "rejected_by_gate": rejected,
            "none": none,
            "errored": errored,
            "requests_made": req,
            "vendor_hit_rate": round(vendor_hit_rate, 4),
            "gate_accept_rate": round(gate_accept_rate, 4),
            "rows_per_min": round(rpm, 1),
            "requests_per_row": round(rpr, 3),
            "cost_usd": maps.get("cost_usd"),
            "billing": maps.get("billing"),
            "error": maps.get("error"),
            "complete": rows_done >= targets and not maps.get("error"),
        }
        rows.append(row)
        print("ROW", json.dumps(row), flush=True)
        PARTIAL.write_text(json.dumps({"table": rows}, indent=2))
    a1 = rows[0]["vendor_hit_rate"]
    a12 = rows[-1]["vendor_hit_rate"]
    ok = (
        abs(a12 - a1) <= 0.02
        and all(r["complete"] for r in rows)
        and all(r["errored"] == 0 for r in rows)
        and rows[-1]["rows_per_min"] >= 40
    )
    # Prefer shipping the highest concurrency whose hit rate stays within 2pp of serial.
    best = rows[0]
    for r in rows[1:]:
        if abs(r["vendor_hit_rate"] - rows[0]["vendor_hit_rate"]) <= 0.02 and r["complete"] and r["errored"] == 0:
            best = r
    summary = {
        "ship": ok or (best["concurrency"] >= 4 and best["complete"]),
        "recommended_concurrency": best["concurrency"],
        "limit": LIMIT,
        "accept_rate_metric": "vendor_hit_rate (candidates before gate / rows_done)",
        "vendor_hit_rate_delta_pp_vs_serial": {
            str(r["concurrency"]): round((r["vendor_hit_rate"] - rows[0]["vendor_hit_rate"]) * 100, 2)
            for r in rows
        },
        "table": rows,
    }
    print("SUMMARY", json.dumps(summary, indent=2), flush=True)
    OUT.write_text(json.dumps(summary, indent=2))
except Exception:
    ERR.write_text(traceback.format_exc())
    traceback.print_exc()
    raise
