-- Domain Waterfall: profiles, safe source IO, domain cache.
-- Writeback is allowlisted. Never touches dl_status, sg_exclude, or skip_*.

CREATE TABLE IF NOT EXISTS public.wf_client_profiles (
  client_tag text PRIMARY KEY,
  display_name text NOT NULL DEFAULT '',
  profile jsonb NOT NULL DEFAULT '{}'::jsonb,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.wf_domain_cache (
  company_name_normalized text PRIMARY KEY,
  domain text NOT NULL,
  source text,
  client_tag text,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION public.dw_ensure_profile(
  p_client_tag text,
  p_display_name text,
  p_profile jsonb
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  tag text;
  rec public.wf_client_profiles;
BEGIN
  tag := lower(regexp_replace(coalesce(p_client_tag, ''), '[^a-z0-9_]', '', 'g'));
  IF tag = '' THEN
    RAISE EXCEPTION 'client_tag is required';
  END IF;
  INSERT INTO public.wf_client_profiles (client_tag, display_name, profile, updated_at)
  VALUES (
    tag,
    coalesce(nullif(p_display_name, ''), tag),
    coalesce(p_profile, '{}'::jsonb) || jsonb_build_object('client_tag', tag),
    now()
  )
  ON CONFLICT (client_tag) DO UPDATE
    SET display_name = excluded.display_name,
        profile = excluded.profile,
        updated_at = now()
  RETURNING * INTO rec;
  RETURN jsonb_build_object(
    'client_tag', rec.client_tag,
    'display_name', rec.display_name,
    'profile', rec.profile
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_get_profile(p_client_tag text)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  tag text;
  rec public.wf_client_profiles;
BEGIN
  tag := lower(regexp_replace(coalesce(p_client_tag, ''), '[^a-z0-9_]', '', 'g'));
  SELECT * INTO rec FROM public.wf_client_profiles WHERE client_tag = tag;
  IF NOT FOUND THEN
    RETURN NULL;
  END IF;
  RETURN jsonb_build_object(
    'client_tag', rec.client_tag,
    'display_name', rec.display_name,
    'profile', rec.profile
  );
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_source_columns(p_schema text, p_table text)
RETURNS text[]
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  sch text;
  tbl text;
  cols text[];
BEGIN
  sch := lower(regexp_replace(coalesce(p_schema, 'public'), '[^a-z0-9_]', '', 'g'));
  tbl := lower(regexp_replace(coalesce(p_table, ''), '[^a-z0-9_]', '', 'g'));
  IF sch = '' OR tbl = '' OR to_regclass(format('%I.%I', sch, tbl)) IS NULL THEN
    RAISE EXCEPTION 'unknown source table %.%', sch, tbl;
  END IF;
  SELECT array_agg(c.column_name::text ORDER BY c.ordinal_position)
    INTO cols
  FROM information_schema.columns c
  WHERE c.table_schema = sch AND c.table_name = tbl;
  RETURN coalesce(cols, ARRAY[]::text[]);
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_read_source(
  p_schema text,
  p_table text,
  p_filters jsonb DEFAULT '[]'::jsonb,
  p_columns text[] DEFAULT NULL,
  p_key_column text DEFAULT 'id',
  p_after text DEFAULT NULL,
  p_limit integer DEFAULT 500
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  sch text;
  tbl text;
  keycol text;
  cols text[];
  filt jsonb;
  clause text := ' WHERE true';
  col text;
  op text;
  val text;
  arr text[];
  sql text;
  result jsonb;
  lim int;
BEGIN
  sch := lower(regexp_replace(coalesce(p_schema, 'public'), '[^a-z0-9_]', '', 'g'));
  tbl := lower(regexp_replace(coalesce(p_table, ''), '[^a-z0-9_]', '', 'g'));
  keycol := lower(regexp_replace(coalesce(p_key_column, 'id'), '[^a-z0-9_]', '', 'g'));
  lim := least(greatest(coalesce(p_limit, 500), 1), 500);
  IF sch = '' OR tbl = '' OR to_regclass(format('%I.%I', sch, tbl)) IS NULL THEN
    RAISE EXCEPTION 'unknown source table %.%', sch, tbl;
  END IF;

  IF p_columns IS NULL OR coalesce(array_length(p_columns, 1), 0) = 0 THEN
    cols := ARRAY[keycol];
  ELSE
    SELECT array_agg(lower(regexp_replace(c, '[^a-z0-9_]', '', 'g')))
      INTO cols
    FROM unnest(p_columns) AS c
    WHERE c IS NOT NULL AND btrim(c) <> '';
    IF cols IS NULL THEN
      cols := ARRAY[keycol];
    ELSIF NOT keycol = ANY (cols) THEN
      cols := cols || keycol;
    END IF;
  END IF;

  FOR filt IN SELECT * FROM jsonb_array_elements(coalesce(p_filters, '[]'::jsonb))
  LOOP
    col := lower(regexp_replace(coalesce(filt->>'col', ''), '[^a-z0-9_]', '', 'g'));
    op := lower(coalesce(filt->>'op', ''));
    val := filt->>'value';
    IF col = '' THEN
      CONTINUE;
    END IF;
    IF op IN ('is.null', 'is null') THEN
      clause := clause || format(' AND %I IS NULL', col);
    ELSIF op IN ('not.is.null', 'is not null') THEN
      clause := clause || format(' AND %I IS NOT NULL', col);
    ELSIF op IN ('eq', '=') THEN
      clause := clause || format(' AND %I = %L', col, val);
    ELSIF op IN ('neq', '!=', '<>') THEN
      clause := clause || format(' AND %I <> %L', col, val);
    ELSIF op = 'in' THEN
      SELECT array_agg(x)
        INTO arr
      FROM jsonb_array_elements_text(coalesce(filt->'value', '[]'::jsonb)) AS x;
      IF arr IS NULL OR array_length(arr, 1) IS NULL THEN
        CONTINUE;
      END IF;
      clause := clause || format(' AND %I IN (%s)', col, (
        SELECT string_agg(quote_literal(a), ', ') FROM unnest(arr) AS a
      ));
    ELSE
      RAISE EXCEPTION 'unsupported filter op: %', op;
    END IF;
  END LOOP;

  IF p_after IS NOT NULL AND btrim(p_after) <> '' THEN
    clause := clause || format(' AND %I::text > %L', keycol, p_after);
  END IF;

  sql := format(
    'SELECT coalesce(jsonb_agg(to_jsonb(t)), ''[]''::jsonb) FROM (SELECT %s FROM %I.%I%s ORDER BY %I ASC LIMIT %s) t',
    (SELECT string_agg(format('%I', c), ', ') FROM unnest(cols) AS c),
    sch, tbl, clause, keycol, lim
  );
  EXECUTE sql INTO result;
  RETURN coalesce(result, '[]'::jsonb);
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_patch_source(
  p_schema text,
  p_table text,
  p_key_column text,
  p_key text,
  p_fields jsonb
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  sch text;
  tbl text;
  keycol text;
  col text;
  sets text := '';
  val jsonb;
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
  sch := lower(regexp_replace(coalesce(p_schema, 'public'), '[^a-z0-9_]', '', 'g'));
  tbl := lower(regexp_replace(coalesce(p_table, ''), '[^a-z0-9_]', '', 'g'));
  keycol := lower(regexp_replace(coalesce(p_key_column, 'id'), '[^a-z0-9_]', '', 'g'));
  IF sch = '' OR tbl = '' OR to_regclass(format('%I.%I', sch, tbl)) IS NULL THEN
    RAISE EXCEPTION 'unknown source table %.%', sch, tbl;
  END IF;
  IF p_key IS NULL OR btrim(p_key) = '' THEN
    RETURN;
  END IF;
  FOR col, val IN SELECT key, value FROM jsonb_each(coalesce(p_fields, '{}'::jsonb))
  LOOP
    col := lower(regexp_replace(col, '[^a-z0-9_]', '', 'g'));
    IF col = '' OR NOT col = ANY (allowed) THEN
      CONTINUE;
    END IF;
    IF col LIKE 'skip_%' OR col IN ('dl_status', 'sg_exclude') THEN
      CONTINUE;
    END IF;
    IF sets <> '' THEN
      sets := sets || ', ';
    END IF;
    IF val IS NULL OR val = 'null'::jsonb THEN
      sets := sets || format('%I = NULL', col);
    ELSIF col = 'wf_domain_candidates' THEN
      sets := sets || format('%I = %L::jsonb', col, val::text);
    ELSIF col = 'wf_domain_agreement' THEN
      sets := sets || format('%I = %L::boolean', col, val #>> '{}');
    ELSIF col = 'wf_domain_confidence' THEN
      sets := sets || format('%I = %L::numeric', col, val #>> '{}');
    ELSE
      sets := sets || format('%I = %L', col, val #>> '{}');
    END IF;
  END LOOP;
  IF sets = '' THEN
    RETURN;
  END IF;
  EXECUTE format('UPDATE %I.%I SET %s WHERE %I::text = %L', sch, tbl, sets, keycol, p_key);
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_ensure_writeback(p_schema text, p_table text)
RETURNS text[]
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  sch text;
  tbl text;
  added text[] := ARRAY[]::text[];
  col text;
  typ text;
BEGIN
  sch := lower(regexp_replace(coalesce(p_schema, 'public'), '[^a-z0-9_]', '', 'g'));
  tbl := lower(regexp_replace(coalesce(p_table, ''), '[^a-z0-9_]', '', 'g'));
  IF sch = '' OR tbl = '' OR to_regclass(format('%I.%I', sch, tbl)) IS NULL THEN
    RAISE EXCEPTION 'unknown source table %.%', sch, tbl;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = sch AND table_name = tbl AND column_name = 'id'
  ) THEN
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN id bigserial', sch, tbl);
    added := added || 'id';
  END IF;
  FOREACH col IN ARRAY ARRAY[
    'wf_domain',
    'wf_domain_source',
    'wf_domain_confidence',
    'wf_domain_agreement',
    'wf_domain_candidates',
    'wf_phone',
    'wf_domain_status'
  ]
  LOOP
    IF EXISTS (
      SELECT 1 FROM information_schema.columns c
      WHERE c.table_schema = sch AND c.table_name = tbl AND c.column_name = col
    ) THEN
      CONTINUE;
    END IF;
    typ := CASE col
      WHEN 'wf_domain_confidence' THEN 'numeric'
      WHEN 'wf_domain_agreement' THEN 'boolean'
      WHEN 'wf_domain_candidates' THEN 'jsonb'
      ELSE 'text'
    END;
    EXECUTE format('ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS %I %s', sch, tbl, col, typ);
    added := added || col;
  END LOOP;
  PERFORM pg_notify('pgrst', 'reload schema');
  RETURN added;
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_cache_remember(
  p_name text,
  p_domain text,
  p_source text,
  p_client_tag text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
BEGIN
  IF coalesce(p_name, '') = '' OR coalesce(p_domain, '') = '' THEN
    RETURN;
  END IF;
  INSERT INTO public.wf_domain_cache (company_name_normalized, domain, source, client_tag, updated_at)
  VALUES (p_name, p_domain, p_source, p_client_tag, now())
  ON CONFLICT (company_name_normalized) DO UPDATE
    SET domain = excluded.domain,
        source = excluded.source,
        client_tag = excluded.client_tag,
        updated_at = now();
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_cache_lookup(p_names text[], p_tables text[] DEFAULT '{}')
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  result jsonb;
BEGIN
  SELECT coalesce(jsonb_agg(jsonb_build_object(
    'company_name_normalized', company_name_normalized,
    'domain', domain,
    'wf_domain', domain
  )), '[]'::jsonb)
    INTO result
  FROM public.wf_domain_cache
  WHERE company_name_normalized = ANY (p_names);
  RETURN coalesce(result, '[]'::jsonb);
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_api_keys()
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public', 'private'
AS $function$
DECLARE
  result jsonb;
BEGIN
  IF to_regclass('private.api_keys') IS NULL THEN
    RETURN '[]'::jsonb;
  END IF;
  SELECT coalesce(jsonb_agg(jsonb_build_object('name', name, 'value', value)), '[]'::jsonb)
    INTO result
  FROM private.api_keys;
  RETURN coalesce(result, '[]'::jsonb);
END;
$function$;

CREATE TABLE IF NOT EXISTS public.goliath_domain_queue (
  id bigserial PRIMARY KEY,
  company_name text,
  city text,
  state text,
  company_name_normalized text,
  phone text,
  zip text,
  street text,
  country text,
  domain text,
  wf_domain text,
  wf_domain_source text,
  wf_domain_confidence numeric,
  wf_domain_agreement boolean,
  wf_domain_candidates jsonb,
  wf_phone text,
  wf_domain_status text
);

GRANT EXECUTE ON FUNCTION public.dw_ensure_profile(text, text, jsonb) TO service_role;
GRANT EXECUTE ON FUNCTION public.dw_get_profile(text) TO service_role;
GRANT EXECUTE ON FUNCTION public.dw_source_columns(text, text) TO service_role;
GRANT EXECUTE ON FUNCTION public.dw_read_source(text, text, jsonb, text[], text, text, integer) TO service_role;
GRANT EXECUTE ON FUNCTION public.dw_patch_source(text, text, text, text, jsonb) TO service_role;
GRANT EXECUTE ON FUNCTION public.dw_ensure_writeback(text, text) TO service_role;
GRANT EXECUTE ON FUNCTION public.dw_cache_remember(text, text, text, text) TO service_role;
GRANT EXECUTE ON FUNCTION public.dw_cache_lookup(text[], text[]) TO service_role;
GRANT EXECUTE ON FUNCTION public.dw_api_keys() TO service_role;

DO $$
BEGIN
  IF to_regclass('client_peterson.gc_adjudication') IS NOT NULL THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_schema = 'client_peterson'
        AND table_name = 'gc_adjudication'
        AND column_name = 'id'
    ) THEN
      ALTER TABLE client_peterson.gc_adjudication ADD COLUMN id bigserial;
    END IF;
  END IF;
END $$;

NOTIFY pgrst, 'reload schema';
