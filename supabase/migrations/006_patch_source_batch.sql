-- Batch writeback: update up to 500 source rows in one statement.
-- Same wf_* allowlist as dw_patch_source. Never touches dl_status, sg_exclude, skip_*.
-- Also exposes column types so the job can preflight before a long/paid tier.

CREATE OR REPLACE FUNCTION public.dw_source_column_types(p_schema text, p_table text)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  sch text;
  tbl text;
  result jsonb;
BEGIN
  sch := lower(regexp_replace(coalesce(p_schema, 'public'), '[^a-z0-9_]', '', 'g'));
  tbl := lower(regexp_replace(coalesce(p_table, ''), '[^a-z0-9_]', '', 'g'));
  IF sch = '' OR tbl = '' OR to_regclass(format('%I.%I', sch, tbl)) IS NULL THEN
    RAISE EXCEPTION 'unknown source table %.%', sch, tbl;
  END IF;
  SELECT coalesce(jsonb_object_agg(c.column_name, c.data_type), '{}'::jsonb)
    INTO result
  FROM information_schema.columns c
  WHERE c.table_schema = sch AND c.table_name = tbl;
  RETURN coalesce(result, '{}'::jsonb);
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_patch_source_batch(
  p_table text,
  p_key_column text,
  p_rows jsonb
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  raw text;
  sch text;
  tbl text;
  keycol text;
  payload jsonb;
  sql text;
  n int := 0;
  allowed text[] := ARRAY[
    'wf_domain',
    'wf_domain_source',
    'wf_domain_confidence',
    'wf_domain_agreement',
    'wf_domain_candidates',
    'wf_phone',
    'wf_domain_status'
  ];
BEGIN
  raw := lower(btrim(coalesce(p_table, '')));
  IF raw = '' THEN
    RAISE EXCEPTION 'p_table is required';
  END IF;
  IF position('.' in raw) > 0 THEN
    sch := split_part(raw, '.', 1);
    tbl := split_part(raw, '.', 2);
  ELSE
    sch := 'public';
    tbl := raw;
  END IF;
  sch := lower(regexp_replace(coalesce(sch, 'public'), '[^a-z0-9_]', '', 'g'));
  tbl := lower(regexp_replace(coalesce(tbl, ''), '[^a-z0-9_]', '', 'g'));
  keycol := lower(regexp_replace(coalesce(p_key_column, 'id'), '[^a-z0-9_]', '', 'g'));
  IF sch = '' OR tbl = '' OR keycol = '' OR to_regclass(format('%I.%I', sch, tbl)) IS NULL THEN
    RAISE EXCEPTION 'unknown source table %.%', sch, tbl;
  END IF;
  IF keycol = ANY (ARRAY['dl_status', 'sg_exclude']) OR keycol LIKE 'skip_%' THEN
    RAISE EXCEPTION 'forbidden key column %', keycol;
  END IF;
  IF jsonb_typeof(coalesce(p_rows, '[]'::jsonb)) <> 'array' THEN
    RAISE EXCEPTION 'p_rows must be a json array';
  END IF;

  SELECT coalesce(jsonb_agg(kept), '[]'::jsonb)
    INTO payload
  FROM (
    SELECT (
      SELECT jsonb_object_agg(k, v)
      FROM jsonb_each(elem) AS e(k, v)
      WHERE lower(k) = keycol OR lower(k) = ANY (allowed)
    ) AS kept
    FROM jsonb_array_elements(coalesce(p_rows, '[]'::jsonb)) AS elem
    LIMIT 500
  ) filtered
  WHERE kept IS NOT NULL AND kept ? keycol;

  IF payload IS NULL OR payload = '[]'::jsonb THEN
    RETURN 0;
  END IF;

  sql := format(
    $q$
    UPDATE %I.%I AS t
    SET
      wf_domain = s.wf_domain,
      wf_domain_source = s.wf_domain_source,
      wf_domain_confidence = s.wf_domain_confidence,
      wf_domain_agreement = s.wf_domain_agreement,
      wf_domain_candidates = s.wf_domain_candidates,
      wf_phone = s.wf_phone,
      wf_domain_status = s.wf_domain_status
    FROM jsonb_to_recordset($1) AS s(
      %I text,
      wf_domain text,
      wf_domain_source text,
      wf_domain_confidence numeric,
      wf_domain_agreement boolean,
      wf_domain_candidates jsonb,
      wf_phone text,
      wf_domain_status text
    )
    WHERE t.%I::text = s.%I
    $q$,
    sch, tbl, keycol, keycol, keycol
  );
  EXECUTE sql USING payload;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END;
$function$;

GRANT EXECUTE ON FUNCTION public.dw_source_column_types(text, text) TO service_role;
GRANT EXECUTE ON FUNCTION public.dw_patch_source_batch(text, text, jsonb) TO service_role;

NOTIFY pgrst, 'reload schema';
