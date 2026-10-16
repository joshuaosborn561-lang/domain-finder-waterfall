-- Partial-row writeback: update only wf_* keys present in each jsonb object.
-- Missing keys keep the existing column value (never null it out).
-- Explicit JSON null still writes SQL NULL. Allowlist unchanged.
-- Never touches dl_status, sg_exclude, skip_*.

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

  -- jsonb_to_recordset turns missing keys into NULL, which would wipe columns.
  -- Drive the UPDATE from jsonb_array_elements and only assign keys that are present.
  sql := format(
    $q$
    UPDATE %I.%I AS t
    SET
      wf_domain = CASE
        WHEN e.elem ? 'wf_domain' THEN e.elem->>'wf_domain'
        ELSE t.wf_domain
      END,
      wf_domain_source = CASE
        WHEN e.elem ? 'wf_domain_source' THEN e.elem->>'wf_domain_source'
        ELSE t.wf_domain_source
      END,
      wf_domain_confidence = CASE
        WHEN e.elem ? 'wf_domain_confidence' THEN
          CASE
            WHEN e.elem->'wf_domain_confidence' = 'null'::jsonb THEN NULL
            ELSE (e.elem->>'wf_domain_confidence')::numeric
          END
        ELSE t.wf_domain_confidence
      END,
      wf_domain_agreement = CASE
        WHEN e.elem ? 'wf_domain_agreement' THEN
          CASE
            WHEN e.elem->'wf_domain_agreement' = 'null'::jsonb THEN NULL
            ELSE (e.elem->>'wf_domain_agreement')::boolean
          END
        ELSE t.wf_domain_agreement
      END,
      wf_domain_candidates = CASE
        WHEN e.elem ? 'wf_domain_candidates' THEN
          CASE
            WHEN e.elem->'wf_domain_candidates' = 'null'::jsonb THEN NULL
            ELSE e.elem->'wf_domain_candidates'
          END
        ELSE t.wf_domain_candidates
      END,
      wf_phone = CASE
        WHEN e.elem ? 'wf_phone' THEN e.elem->>'wf_phone'
        ELSE t.wf_phone
      END,
      wf_domain_status = CASE
        WHEN e.elem ? 'wf_domain_status' THEN e.elem->>'wf_domain_status'
        ELSE t.wf_domain_status
      END
    FROM jsonb_array_elements($1) AS e(elem)
    WHERE t.%I::text = e.elem->>%L
    $q$,
    sch, tbl, keycol, keycol
  );
  EXECUTE sql USING payload;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END;
$function$;

GRANT EXECUTE ON FUNCTION public.dw_patch_source_batch(text, text, jsonb) TO service_role;

NOTIFY pgrst, 'reload schema';
