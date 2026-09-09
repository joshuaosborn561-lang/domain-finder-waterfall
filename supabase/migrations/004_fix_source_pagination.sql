-- Keyset pagination must use the same type as ORDER BY.
-- id::text > '500' drops 1000+ (text '1000' < '500') and then the
-- Python client stops on a short page. That silently halved the pool.

CREATE OR REPLACE FUNCTION public.dw_filter_clause(p_filters jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $function$
DECLARE
  filt jsonb;
  col text;
  op text;
  val text;
  arr text[];
  clause text := ' WHERE true';
BEGIN
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
  RETURN clause;
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_count_source(
  p_schema text,
  p_table text,
  p_filters jsonb DEFAULT '[]'::jsonb
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  sch text;
  tbl text;
  sql text;
  n int;
BEGIN
  sch := lower(regexp_replace(coalesce(p_schema, 'public'), '[^a-z0-9_]', '', 'g'));
  tbl := lower(regexp_replace(coalesce(p_table, ''), '[^a-z0-9_]', '', 'g'));
  IF sch = '' OR tbl = '' OR to_regclass(format('%I.%I', sch, tbl)) IS NULL THEN
    RAISE EXCEPTION 'unknown source table %.%', sch, tbl;
  END IF;
  sql := format(
    'SELECT count(*)::int FROM %I.%I%s',
    sch, tbl, public.dw_filter_clause(p_filters)
  );
  EXECUTE sql INTO n;
  RETURN coalesce(n, 0);
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
  clause text;
  sql text;
  result jsonb;
  lim int;
  key_type text;
  order_expr text;
  after_clause text := '';
BEGIN
  sch := lower(regexp_replace(coalesce(p_schema, 'public'), '[^a-z0-9_]', '', 'g'));
  tbl := lower(regexp_replace(coalesce(p_table, ''), '[^a-z0-9_]', '', 'g'));
  keycol := lower(regexp_replace(coalesce(p_key_column, 'id'), '[^a-z0-9_]', '', 'g'));
  lim := least(greatest(coalesce(p_limit, 500), 1), 500);
  IF sch = '' OR tbl = '' OR to_regclass(format('%I.%I', sch, tbl)) IS NULL THEN
    RAISE EXCEPTION 'unknown source table %.%', sch, tbl;
  END IF;

  SELECT c.data_type INTO key_type
  FROM information_schema.columns c
  WHERE c.table_schema = sch AND c.table_name = tbl AND c.column_name = keycol;

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

  clause := public.dw_filter_clause(p_filters);

  IF key_type IN ('integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'double precision') THEN
    order_expr := format('%I ASC', keycol);
    IF p_after IS NOT NULL AND btrim(p_after) <> '' THEN
      after_clause := format(' AND %I > %L::numeric', keycol, p_after);
    END IF;
  ELSIF key_type = 'uuid' THEN
    order_expr := format('%I ASC', keycol);
    IF p_after IS NOT NULL AND btrim(p_after) <> '' THEN
      after_clause := format(' AND %I > %L::uuid', keycol, p_after);
    END IF;
  ELSE
    order_expr := format('%I::text ASC', keycol);
    IF p_after IS NOT NULL AND btrim(p_after) <> '' THEN
      after_clause := format(' AND %I::text > %L', keycol, p_after);
    END IF;
  END IF;

  sql := format(
    'SELECT coalesce(jsonb_agg(to_jsonb(t)), ''[]''::jsonb) FROM (SELECT %s FROM %I.%I%s%s ORDER BY %s LIMIT %s) t',
    (SELECT string_agg(format('%I', c), ', ') FROM unnest(cols) AS c),
    sch, tbl, clause, after_clause, order_expr, lim
  );
  EXECUTE sql INTO result;
  RETURN coalesce(result, '[]'::jsonb);
END;
$function$;

CREATE OR REPLACE FUNCTION public.dw_defer_unfetched(
  p_schema text,
  p_table text,
  p_filters jsonb,
  p_key_column text,
  p_keep_keys text[]
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
DECLARE
  sch text;
  tbl text;
  keycol text;
  sql text;
  n int;
BEGIN
  sch := lower(regexp_replace(coalesce(p_schema, 'public'), '[^a-z0-9_]', '', 'g'));
  tbl := lower(regexp_replace(coalesce(p_table, ''), '[^a-z0-9_]', '', 'g'));
  keycol := lower(regexp_replace(coalesce(p_key_column, 'id'), '[^a-z0-9_]', '', 'g'));
  IF sch = '' OR tbl = '' OR to_regclass(format('%I.%I', sch, tbl)) IS NULL THEN
    RAISE EXCEPTION 'unknown source table %.%', sch, tbl;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = sch AND table_name = tbl AND column_name = 'wf_domain_status'
  ) THEN
    RETURN 0;
  END IF;
  sql := format(
    'UPDATE %I.%I SET wf_domain_status = ''deferred''
      %s AND %I::text <> ALL (%L::text[])
      AND (wf_domain_status IS NULL OR wf_domain_status NOT IN (''resolved'', ''review''))',
    sch, tbl, public.dw_filter_clause(p_filters), keycol, coalesce(p_keep_keys, ARRAY[]::text[])
  );
  EXECUTE sql;
  GET DIAGNOSTICS n = ROW_COUNT;
  RETURN n;
END;
$function$;

GRANT EXECUTE ON FUNCTION public.dw_filter_clause(jsonb) TO service_role, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.dw_count_source(text, text, jsonb) TO service_role, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.dw_read_source(text, text, jsonb, text[], text, text, integer) TO service_role, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.dw_defer_unfetched(text, text, jsonb, text, text[]) TO service_role, anon, authenticated;
