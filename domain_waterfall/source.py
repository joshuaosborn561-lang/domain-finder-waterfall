"""Read source tables server-side. Never return row payloads on the MCP surface."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

from . import supabase as sb
from .config import DEFAULT_SUPABASE_PROJECT
from .normalize import normalize_name

log = logging.getLogger("domain_waterfall.source")

PAGE_SIZE = 500
INPUT_FIELDS = (
    "company_name",
    "city",
    "state",
    "company_name_normalized",
    "phone",
    "zip",
    "street",
    "country",
    "domain",
)
FIELD_CANDIDATES: dict[str, tuple[str, ...]] = {
    "company_name": ("company_name", "contractor_name", "business_name", "name"),
    "city": ("city", "address_city"),
    "state": ("state", "address_state"),
    "company_name_normalized": ("company_name_normalized",),
    "phone": ("phone", "wf_phone"),
    "zip": ("zip", "postal_code"),
    "street": ("street", "address", "street_address"),
    "country": ("country",),
    "domain": ("domain", "website"),
}
# id first for tables that already have it. place_id covers Maps-sourced queues.
KEY_CANDIDATES = ("id", "place_id", "pk", "row_id", "uuid")
PROFILE_COLUMN_KEYS: dict[str, tuple[str, ...]] = {
    "company_name": ("name_column", "company_name_column"),
    "city": ("city_column",),
    "state": ("state_column",),
    "domain": ("domain_column",),
    "phone": ("phone_column",),
    "zip": ("zip_column",),
    "street": ("street_column", "address_column"),
}

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PRED = re.compile(
    r"""
    (?P<col>[A-Za-z_][A-Za-z0-9_]*)
    \s+
    (?:
        (?P<null>is\s+not\s+null|is\s+null)
        |
        (?P<in>in)
        \s*\((?P<list>[^)]+)\)
        |
        (?P<op>=|!=|<>)
        \s+
        (?:'(?P<q>(?:[^']|'')*)'|(?P<num>-?\d+(?:\.\d+)?))
    )
    """,
    re.I | re.X,
)


@dataclass
class TableSource:
    project_id: str
    schema: str = "public"
    table: str = ""
    where: str = ""
    key_column: str = "id"
    column_map: dict[str, str] = field(default_factory=dict)
    column_overrides: dict[str, str] = field(default_factory=dict)
    available_columns: set[str] = field(default_factory=set)
    limit: int | None = None
    map_explicit: bool = False

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.table}"


def split_qualified(name: str, default_schema: str = "public") -> tuple[str, str]:
    raw = (name or "").strip()
    default = (default_schema or "public").strip() or "public"
    if not raw:
        return default, ""
    if raw.count(".") == 1:
        schema, table = raw.split(".", 1)
        return schema.strip(), table.strip()
    if "." in raw:
        raise ValueError("source_table must be table or schema.table")
    return default, raw


def parse_source(
    source_table: str,
    where: str = "",
    *,
    limit: int | None = None,
    key_column: str = "id",
) -> TableSource:
    schema, table = split_qualified(source_table)
    if not table or not _IDENT.match(table) or not _IDENT.match(schema):
        raise ValueError("source_table must be table or schema.table")
    if not _IDENT.match(key_column):
        raise ValueError("key_column must be an identifier")
    return TableSource(
        project_id=DEFAULT_SUPABASE_PROJECT,
        schema=schema,
        table=table,
        where=(where or "").strip(),
        key_column=key_column,
        column_map={f: f for f in INPUT_FIELDS},
        limit=limit,
    )


def where_to_filters(where: str) -> list[dict[str, Any]]:
    text = (where or "").strip()
    if not text:
        return []
    parts = re.split(r"\s+and\s+", text, flags=re.I)
    out: list[dict[str, Any]] = []
    for part in parts:
        part = part.strip().rstrip(";")
        if not part:
            continue
        m = _PRED.fullmatch(part)
        if not m:
            raise ValueError(
                "where only allows AND predicates: is null / is not null / = / != / in (...)"
            )
        col = m.group("col")
        if m.group("null"):
            null_op = m.group("null").lower()
            out.append(
                {"col": col, "op": "is null" if null_op == "is null" else "is not null"}
            )
            continue
        if m.group("in"):
            items = []
            for raw in m.group("list").split(","):
                item = raw.strip()
                if item.startswith("'") and item.endswith("'"):
                    items.append(item[1:-1].replace("''", "'"))
                else:
                    items.append(item)
            out.append({"col": col, "op": "in", "value": items})
            continue
        op = m.group("op")
        value = m.group("q")
        if value is not None:
            value = value.replace("''", "'")
        else:
            value = m.group("num")
        out.append({"col": col, "op": "eq" if op == "=" else "neq", "value": value})
    return out


def _coerce_columns(data: Any) -> set[str]:
    if isinstance(data, dict):
        data = (
            data.get("dw_source_columns")
            or data.get("columns")
            or data.get("cols")
            or data
        )
    if isinstance(data, list) and data and isinstance(data[0], dict):
        nested = data[0].get("dw_source_columns") or data[0].get("columns") or data[0].get("cols")
        if nested is not None:
            data = nested
    if isinstance(data, str):
        text = data.strip()
        if text.startswith("{") and text.endswith("}"):
            text = text[1:-1]
        data = [p.strip().strip('"') for p in text.split(",") if p.strip()]
    if isinstance(data, list):
        return {str(c) for c in data if c and not isinstance(c, dict)}
    return set()


def list_columns(src: TableSource) -> set[str]:
    data = sb.rpc("dw_source_columns", {"p_schema": src.schema, "p_table": src.table})
    return _coerce_columns(data)


def profile_column_overrides(raw: dict[str, Any] | None) -> dict[str, str]:
    blob = raw if isinstance(raw, dict) else {}
    out: dict[str, str] = {}
    for field_name, keys in PROFILE_COLUMN_KEYS.items():
        for key in keys:
            val = blob.get(key)
            if val and _IDENT.match(str(val).strip()):
                out[field_name] = str(val).strip()
                break
    return out


def apply_column_overrides(src: TableSource, raw: dict[str, Any] | None) -> None:
    """Record profile column names. discover_column_map applies them after listing cols."""
    src.column_overrides = profile_column_overrides(raw)
    blob = raw if isinstance(raw, dict) else {}
    key = blob.get("key_column") or blob.get("id_column")
    if key and _IDENT.match(str(key).strip()):
        src.key_column = str(key).strip()


def discover_column_map(src: TableSource) -> None:
    cols = list_columns(src)
    if not cols:
        raise ValueError(f"no columns found for {src.qualified}")
    src.available_columns = set(cols)
    mapping: dict[str, str] = {}
    for field_name, candidates in FIELD_CANDIDATES.items():
        for cand in candidates:
            if cand in cols:
                mapping[field_name] = cand
                break
    for field_name, col in src.column_overrides.items():
        if col not in cols:
            raise ValueError(
                f"profile {field_name} column {col!r} is not on {src.qualified}, "
                f"columns={sorted(cols)}"
            )
        mapping[field_name] = col
    src.column_map = mapping
    if src.key_column not in cols:
        picked = next((c for c in KEY_CANDIDATES if c in cols), "")
        if not picked:
            raise ValueError(
                f"{src.qualified} has no usable key column "
                f"(looked for {', '.join(KEY_CANDIDATES)}), "
                f"columns={sorted(cols)}"
            )
        src.key_column = picked


def describe_filter_sql(where: str) -> str:
    parts = ["true"]
    for filt in where_to_filters(where):
        col = filt["col"]
        op = filt["op"]
        if op == "is null":
            parts.append(f"{col} IS NULL")
        elif op == "is not null":
            parts.append(f"{col} IS NOT NULL")
        elif op == "eq":
            parts.append(f"{col} = {filt['value']!r}")
        elif op == "neq":
            parts.append(f"{col} <> {filt['value']!r}")
        elif op == "in":
            items = ", ".join(repr(x) for x in filt.get("value") or [])
            parts.append(f"{col} IN ({items})")
        else:
            parts.append(f"{col} {op}")
    return " WHERE " + " AND ".join(parts)


def describe_count_sql(src: TableSource) -> str:
    return f"SELECT count(*) FROM {src.schema}.{src.table}{describe_filter_sql(src.where)}"


def describe_read_sql(src: TableSource) -> str:
    cols = _select_list(src)
    select = ", ".join(cols) if cols else src.key_column
    return (
        f"SELECT {select} FROM {src.schema}.{src.table}"
        f"{describe_filter_sql(src.where)} ORDER BY {src.key_column}"
    )


def _select_list(src: TableSource) -> list[str]:
    cols = {src.key_column, *src.column_map.values()}
    return sorted(c for c in cols if c)


@dataclass
class FetchResult:
    rows: list[dict[str, Any]]
    rows_matched: int
    rows_fetched: int
    rows_excluded: int
    exclusion_reasons: dict[str, int]

    def to_public(self) -> dict[str, Any]:
        return {
            "rows_matched": self.rows_matched,
            "rows_fetched": self.rows_fetched,
            "rows_excluded": self.rows_excluded,
            "exclusion_reasons": dict(self.exclusion_reasons),
        }

    def assert_explained(self) -> None:
        explained = sum(self.exclusion_reasons.values())
        if self.rows_excluded > 0 and explained != self.rows_excluded:
            raise ValueError(
                "unexplained row loss: "
                f"rows_matched={self.rows_matched} rows_fetched={self.rows_fetched} "
                f"rows_excluded={self.rows_excluded} reasons={self.exclusion_reasons} "
                f"reasons_sum={explained}"
            )


def _rpc_rows(data: Any) -> list[Any]:
    if isinstance(data, dict):
        data = data.get("dw_read_source") or data.get("data") or data
    return data if isinstance(data, list) else []


def count_source_rows(src: TableSource) -> int:
    discover_column_map(src)
    data = sb.rpc(
        "dw_count_source",
        {
            "p_schema": src.schema,
            "p_table": src.table,
            "p_filters": where_to_filters(src.where),
        },
    )
    if isinstance(data, int):
        return data
    if isinstance(data, dict):
        val = data.get("dw_count_source")
        if isinstance(val, int):
            return val
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, int):
            return first
        if isinstance(first, dict):
            for key in ("dw_count_source", "count", "n"):
                if isinstance(first.get(key), int):
                    return int(first[key])
    try:
        return int(data)
    except (TypeError, ValueError):
        raise RuntimeError(f"dw_count_source returned {type(data).__name__}") from None


def fetch_source_rows(src: TableSource) -> FetchResult:
    discover_column_map(src)
    filters = where_to_filters(src.where)
    matched = count_source_rows(src)
    mapped: list[dict[str, Any]] = []
    seen: set[str] = set()
    reasons: dict[str, int] = {}
    cursor: str | None = None
    hit_limit = False

    while True:
        if src.limit is not None and len(mapped) >= src.limit:
            hit_limit = True
            break
        page = PAGE_SIZE
        data = sb.rpc(
            "dw_read_source",
            {
                "p_schema": src.schema,
                "p_table": src.table,
                "p_filters": filters,
                "p_columns": _select_list(src),
                "p_key_column": src.key_column,
                "p_after": cursor,
                "p_limit": page,
            },
        )
        rows = _rpc_rows(data)
        if not rows:
            break
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            raw_key = raw.get(src.key_column)
            if raw_key is None or str(raw_key).strip() == "":
                reasons["missing_source_key"] = reasons.get("missing_source_key", 0) + 1
                continue
            key = str(raw_key)
            cursor = key
            if key in seen:
                reasons["duplicate_source_key"] = reasons.get("duplicate_source_key", 0) + 1
                continue
            if src.limit is not None and len(mapped) >= src.limit:
                hit_limit = True
                break
            seen.add(key)
            item: dict[str, Any] = {"_source_key": raw_key}
            for field_name, col in src.column_map.items():
                item[field_name] = raw.get(col)
            name = str(item.get("company_name") or "").strip()
            if not item.get("company_name_normalized"):
                item["company_name_normalized"] = normalize_name(name)
            mapped.append(item)
        if hit_limit:
            break
        if len(rows) < page:
            break

    if src.limit is not None and (hit_limit or len(mapped) >= src.limit):
        leftover = matched - len(mapped) - sum(reasons.values())
        if leftover > 0:
            reasons["job_limit"] = leftover
            hit_limit = True

    fetched = len(mapped)
    excluded = matched - fetched
    if excluded < 0:
        raise ValueError(
            f"fetch produced more rows than count(*): matched={matched} fetched={fetched}"
        )
    result = FetchResult(
        rows=mapped,
        rows_matched=matched,
        rows_fetched=fetched,
        rows_excluded=excluded,
        exclusion_reasons=reasons,
    )
    result.assert_explained()
    return result


def defer_unfetched(src: TableSource, keep_keys: list[str]) -> int:
    data = sb.rpc(
        "dw_defer_unfetched",
        {
            "p_schema": src.schema,
            "p_table": src.table,
            "p_filters": where_to_filters(src.where),
            "p_key_column": src.key_column,
            "p_keep_keys": [str(k) for k in keep_keys],
        },
    )
    if isinstance(data, int):
        return data
    if isinstance(data, dict) and isinstance(data.get("dw_defer_unfetched"), int):
        return int(data["dw_defer_unfetched"])
    try:
        return int(data or 0)
    except (TypeError, ValueError):
        return 0


def ensure_writeback(src: TableSource) -> list[str]:
    data = sb.rpc(
        "dw_ensure_writeback",
        {"p_schema": src.schema, "p_table": src.table},
    )
    if isinstance(data, list):
        return [str(x) for x in data]
    return []


def patch_source_row(src: TableSource, key: Any, fields: dict[str, Any]) -> None:
    allowed = sb.filter_write_fields(fields)
    if not allowed or key in (None, ""):
        return
    sb.rpc(
        "dw_patch_source",
        {
            "p_schema": src.schema,
            "p_table": src.table,
            "p_key_column": src.key_column,
            "p_key": str(key),
            "p_fields": allowed,
        },
    )
