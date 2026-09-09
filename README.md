# Domain Waterfall MCP

Standalone Railway service. One job: take a **company name plus a location** and return a domain we can trust, or say clearly that it could not.

It never finds a person and never finds an email. Those are later waterfalls.

Nothing industry-specific lives in code. Every client, vertical, and geography is a JSON profile in `public.wf_client_profiles` (`ensure_profile` / `get_profile`). Domain fields are deep-merged so a shared people-waterfall profile is not overwritten. Domain cache tables live in `domain_cache_tables`.

## Tools

| Tool | Purpose |
|---|---|
| `ensure_profile(client_tag, profile_json)` | Upsert the profile document |
| `get_profile(client_tag)` | Read it back |
| `receipt_test(client_tag, n)` | Phase zero. Score each tier on ground truth, with and without location |
| `resolve_domain(source_table, where, client_tag, max_tier, approve_cost_usd, estimate_only)` | Production run. Source table only. Counts / cost, never rows |
| `get_job_status(job_id)` | Last known progress. Never a bare error |
| `list_jobs(limit)` | Recent jobs |

Input rows are always read via `source_table` + `where`, paged 500 server-side.

Writeback columns only: `wf_domain`, `wf_domain_source`, `wf_domain_confidence`, `wf_domain_agreement`, `wf_domain_candidates`, `wf_phone`, `wf_domain_status`. The patch RPC will not touch `dl_status`, `sg_exclude`, or `skip_*`.

## Tiers

Cheapest first. Free tiers, then paid tiers sorted by **live** unit price at job start. Free-on-miss sorts as unit × measured hit rate (defaults to half until a receipt runs).

`cache` → `maps` → `aiark` → `discolike` → `serp` → `prospeo` → `leadmagic`

A receipt run drops a tier from the profile when it returned zero correct hits, and writes `hit_rates` + `tier_order`.

## Acceptance gate

Every vendor output passes the same gate or does not count: aggregator blocklist, TLD allow list, distinctive-token check, industry reject/allow from the profile, geography (area code / state), then sink detection (>3 distinct input names → one domain is nulled).

## Local

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
pytest -q
python -m mcp_server
```

## Railway

```bash
railway init --name domain-waterfall
railway up
railway domain
railway variables set MCP_TRANSPORT=streamable-http SUPABASE_URL=… SUPABASE_SERVICE_ROLE_KEY=…
```

Dockerfile binds `HOST=0.0.0.0` / `PORT`. Health: `GET /health`. MCP: `/mcp`.
