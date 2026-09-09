-- Cache lookup also scans profile domain_cache_tables (schema.table).
-- Name match uses the same normalize as Python: lower, non-alnum → space.

CREATE OR REPLACE FUNCTION public.dw_norm_name(p_name text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $function$
  SELECT trim(both ' ' FROM regexp_replace(
    regexp_replace(lower(coalesce(p_name, '')), '[^a-z0-9]+', ' ', 'g'),
    '\s+', ' ', 'g'
  ));
$function$;

CREATE OR REPLACE FUNCTION public.dw_cache_lookup(p_names text[], p_tables text[] DEFAULT '{}')
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  wanted text[];
  result jsonb := '[]'::jsonb;
  extra jsonb;
  raw text;
  sch text;
  tbl text;
  name_col text;
  domain_col text;
  sql text;
BEGIN
  SELECT array_agg(public.dw_norm_name(x))
    INTO wanted
  FROM unnest(coalesce(p_names, ARRAY[]::text[])) AS x
  WHERE public.dw_norm_name(x) <> '';
  IF wanted IS NULL THEN
    RETURN '[]'::jsonb;
  END IF;

  SELECT coalesce(jsonb_agg(jsonb_build_object(
    'company_name_normalized', company_name_normalized,
    'domain', domain,
    'wf_domain', domain
  )), '[]'::jsonb)
    INTO result
  FROM public.wf_domain_cache
  WHERE company_name_normalized = ANY (wanted);

  FOREACH raw IN ARRAY coalesce(p_tables, ARRAY[]::text[])
  LOOP
    raw := btrim(raw);
    IF raw = '' OR raw NOT LIKE '%.%' THEN
      CONTINUE;
    END IF;
    sch := lower(regexp_replace(split_part(raw, '.', 1), '[^a-z0-9_]', '', 'g'));
    tbl := lower(regexp_replace(split_part(raw, '.', 2), '[^a-z0-9_]', '', 'g'));
    IF sch = '' OR tbl = '' OR to_regclass(format('%I.%I', sch, tbl)) IS NULL THEN
      CONTINUE;
    END IF;
    SELECT c.column_name INTO name_col
    FROM information_schema.columns c
    WHERE c.table_schema = sch AND c.table_name = tbl
      AND c.column_name IN (
        'company_name_normalized', 'company_name', 'contractor_name', 'business_name', 'name'
      )
    ORDER BY CASE c.column_name
      WHEN 'company_name_normalized' THEN 0
      WHEN 'company_name' THEN 1
      WHEN 'contractor_name' THEN 2
      WHEN 'business_name' THEN 3
      ELSE 4
    END
    LIMIT 1;
    SELECT c.column_name INTO domain_col
    FROM information_schema.columns c
    WHERE c.table_schema = sch AND c.table_name = tbl
      AND c.column_name IN ('wf_domain', 'domain', 'website')
    ORDER BY CASE c.column_name
      WHEN 'wf_domain' THEN 0
      WHEN 'domain' THEN 1
      ELSE 2
    END
    LIMIT 1;
    IF name_col IS NULL OR domain_col IS NULL THEN
      CONTINUE;
    END IF;
    sql := format(
      'SELECT coalesce(jsonb_agg(jsonb_build_object(
          ''company_name_normalized'', public.dw_norm_name(%I::text),
          ''domain'', lower(%I::text),
          ''wf_domain'', lower(%I::text)
        )), ''[]''::jsonb)
       FROM %I.%I
       WHERE coalesce(%I::text, '''') <> ''''
         AND public.dw_norm_name(%I::text) = ANY (%L::text[])',
      name_col, domain_col, domain_col, sch, tbl, domain_col, name_col, wanted
    );
    extra := '[]'::jsonb;
    BEGIN
      EXECUTE sql INTO extra;
    EXCEPTION WHEN OTHERS THEN
      extra := '[]'::jsonb;
    END;
    result := coalesce(result, '[]'::jsonb) || coalesce(extra, '[]'::jsonb);
  END LOOP;

  RETURN coalesce(result, '[]'::jsonb);
END;
$function$;

GRANT EXECUTE ON FUNCTION public.dw_norm_name(text) TO service_role;
GRANT EXECUTE ON FUNCTION public.dw_cache_lookup(text[], text[]) TO service_role;
