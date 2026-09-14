#!/usr/bin/env python3
"""Quick rebench at concurrency 8 and 12 after soft throttle tuning."""
from __future__ import annotations

import json
import time
from pathlib import Path

from domain_waterfall.config import load_settings
import domain_waterfall.config as cfg

cfg.settings = load_settings()
from domain_waterfall.waterfall import resolve_domain

LIMIT = 80
rows = []
for conc in (8, 12):
    print(f"START conc={conc}", flush=True)
    t0 = time.time()
    out = resolve_domain(
        source_table="client_peterson.improved_domain_resolution",
        where="domain is null",
        client_tag="peterson_earthworks",
        max_tier="maps",
        min_tier="maps",
        writeback=False,
        limit=LIMIT,
        concurrency=conc,
    )
    elapsed = time.time() - t0
    maps = next(t for t in out["tiers"] if t["tier"] == "maps")
    rows_done = int(maps["rows_done"])
    accepted = int(maps["accepted"])
    rejected = int(maps.get("rejected_by_gate") or 0)
    row = {
        "concurrency": conc,
        "elapsed_s": round(elapsed, 1),
        "rows_done": rows_done,
        "accepted": accepted,
        "rejected_by_gate": rejected,
        "none": maps["none"],
        "errored": maps["errored"],
        "requests_made": maps["requests_made"],
        "vendor_hit_rate": round((accepted + rejected) / rows_done, 4) if rows_done else 0,
        "rows_per_min": round(rows_done / elapsed * 60, 1) if elapsed else 0,
        "requests_per_row": round(maps["requests_made"] / rows_done, 3) if rows_done else 0,
        "cost_usd": maps["cost_usd"],
        "billing": maps["billing"],
        "complete": rows_done >= int(maps["targets"]) and not maps.get("error"),
    }
    rows.append(row)
    print("ROW", json.dumps(row), flush=True)

summary = {"table": rows, "serial_baseline_vendor_hit_rate": 0.2895}
Path("/opt/cursor/artifacts/dw_concurrency_rebench.json").write_text(json.dumps(summary, indent=2))
print("SUMMARY", json.dumps(summary, indent=2), flush=True)
