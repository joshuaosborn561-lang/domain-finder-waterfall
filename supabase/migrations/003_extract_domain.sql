CREATE OR REPLACE FUNCTION public.dw_extract_domain(p_value text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $function$
  SELECT NULLIF(lower(regexp_replace(
    regexp_replace(
      regexp_replace(btrim(coalesce(p_value, '')), '^https?://', '', 'i'),
      '^www\.', '', 'i'
    ),
    '[/?#].*$', ''
  )), '');
$function$;

CREATE OR REPLACE FUNCTION public.dw_tokens(p_name text, p_strip text[])
RETURNS text[]
LANGUAGE sql
IMMUTABLE
AS $function$
  SELECT coalesce(array_agg(w), ARRAY[]::text[])
  FROM (
    SELECT w FROM unnest(string_to_array(public.dw_norm_name(p_name), ' ')) AS w
    WHERE length(w) >= 3
      AND NOT w = ANY (SELECT public.dw_norm_name(s) FROM unnest(coalesce(p_strip, ARRAY[]::text[])) s)
  ) t;
$function$;

GRANT EXECUTE ON FUNCTION public.dw_extract_domain(text) TO service_role, anon, authenticated;
GRANT EXECUTE ON FUNCTION public.dw_tokens(text, text[]) TO service_role, anon, authenticated;
