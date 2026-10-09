-- Strip dropped LeadMagic company-search from stored domain profiles.
-- OPERATOR-RUN ONLY. This PR does not execute this statement.
-- Do not run from CI, Railway deploy, or seed-profiles.
--
-- Scope: public.wf_client_profiles.profile JSON only (e.g. goliath, peterson_roof).
-- Never touches source tables. Never touches dl_status, sg_exclude, or skip_*.
-- Idempotent: safe to run more than once.

-- Preview (read-only):
SELECT
  client_tag,
  profile -> 'enabled_tiers' AS enabled_tiers,
  profile -> 'tier_order' AS tier_order,
  profile -> 'dropped_tiers' AS dropped_tiers,
  profile -> 'hit_rates' AS hit_rates
FROM public.wf_client_profiles
WHERE profile::text ILIKE '%leadmagic%';

-- Apply (profiles only):
UPDATE public.wf_client_profiles AS p
SET
  profile = p.profile || jsonb_strip_nulls(
    jsonb_build_object(
      'enabled_tiers', CASE
        WHEN p.profile ? 'enabled_tiers'
             AND jsonb_typeof(p.profile -> 'enabled_tiers') = 'array'
        THEN coalesce((
          SELECT jsonb_agg(e)
          FROM jsonb_array_elements(p.profile -> 'enabled_tiers') AS e
          WHERE lower(e #>> '{}') IS DISTINCT FROM 'leadmagic'
        ), '[]'::jsonb)
        ELSE NULL
      END,
      'dropped_tiers', CASE
        WHEN p.profile ? 'dropped_tiers'
             AND jsonb_typeof(p.profile -> 'dropped_tiers') = 'array'
        THEN coalesce((
          SELECT jsonb_agg(e)
          FROM jsonb_array_elements(p.profile -> 'dropped_tiers') AS e
          WHERE lower(e #>> '{}') IS DISTINCT FROM 'leadmagic'
        ), '[]'::jsonb)
        ELSE NULL
      END,
      'hit_rates', CASE
        WHEN jsonb_typeof(p.profile -> 'hit_rates') = 'object'
        THEN (p.profile -> 'hit_rates') - 'leadmagic' - 'LeadMagic' - 'LEADMAGIC'
        ELSE NULL
      END,
      'tier_order', CASE
        WHEN p.profile ? 'tier_order'
             AND jsonb_typeof(p.profile -> 'tier_order') = 'array'
        THEN coalesce((
          SELECT jsonb_agg(e)
          FROM jsonb_array_elements(p.profile -> 'tier_order') AS e
          WHERE CASE
            WHEN jsonb_typeof(e) = 'string'
              THEN lower(e #>> '{}') IS DISTINCT FROM 'leadmagic'
            WHEN jsonb_typeof(e) = 'object'
              THEN lower(coalesce(e ->> 'tier', '')) IS DISTINCT FROM 'leadmagic'
            ELSE true
          END
        ), '[]'::jsonb)
        ELSE NULL
      END
    )
  ),
  updated_at = now()
WHERE p.profile::text ILIKE '%leadmagic%';
