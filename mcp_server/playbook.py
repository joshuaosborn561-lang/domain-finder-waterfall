INSTRUCTIONS = """# Domain Waterfall

Takes a company name plus a location and returns a domain we can trust, or says it could not.
It never finds a person and never finds an email.

Every client is a JSON profile in public.wf_client_profiles. Nothing industry-specific lives in code.

Tools:
- ensure_profile / get_profile
- receipt_test (phase zero — run this first)
- resolve_domain(source_table, where, client_tag, max_tier, min_tier, skip_tiers, approve_cost_usd, estimate_only)
- get_job_status / list_jobs (in-tier: processed / rows_done / requests_made / last_progress_at)

Source is always source_table + where, paged 500. No inline rows. Responses are counts/cost only.

Writeback columns only: wf_domain, wf_domain_source, wf_domain_confidence, wf_domain_agreement,
wf_domain_candidates, wf_phone, wf_domain_status.
Never touch dl_status, sg_exclude, or skip_*.
"""

WHEN_TO_USE = """Use this MCP when you have a company name and a location and need a trusted website domain.
Do not use it to find people or emails. Those are later waterfalls.
"""
