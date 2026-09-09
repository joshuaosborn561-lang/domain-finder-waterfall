"""Read source tables server-side. Never return row payloads on the MCP surface."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

from . import supabase as sb
from .config import DEFAULT_SUPABASE_PROJECT
from .normalize import normalize_name

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


def list_columns(src: TableSource) -> set[str]:
    data = sb.rpc("dw_source_columns", {"p_schema": src.schema, "p_table": src.table})
    if isinstance(data, dict):
        data = data.get("dw_source_columns") or data.get("columns") or data
    if isinstance(data, list) and data and isinstance(data[0], dict):
        data = data[0].get("dw_source_columns") or data
    if isinstance(data, list):
        return {str(c) for c in data if c}
    return set()


def discover_column_map(src: TableSource) -> None:
    cols = list_columns(src)
    if not cols:
        return
    mapping: dict[str, str] = {}
    for field_name, candidates in FIELD_CANDIDATES.items():
        for cand in candidates:
            if cand in cols:
                mapping[field_name] = cand
                break
    src.column_map = mapping
    if src.key_column not in cols:
        for cand in ("id", "pk", "row_id", "company_name", "contractor_name"):
            if cand in cols:
                src.key_column = cand
                break


def _select_list(src: TableSource) -> list[str]:
    cols = {src.key_column, *src.column_map.values()}
    return sorted(c for c in cols if c)


def fetch_source_rows(src: TableSource) -> list[dict[str, Any]]:
    discover_column_map(src)
    filters = where_to_filters(src.where)
    mapped: list[dict[str, Any]] = []
    cursor: str | None = None
    remaining = src.limit
    while True:
        page = PAGE_SIZE if remaining is None else min(PAGE_SIZE, remaining)
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
        if isinstance(data, dict):
            data = data.get("dw_read_source") or data.get("data") or data
        rows = data if isinstance(data, list) else []
        if not rows:
            break
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            item: dict[str, Any] = {"_source_key": raw.get(src.key_column)}
            for field_name, col in src.column_map.items():
                item[field_name] = raw.get(col)
            name = str(item.get("company_name") or "").strip()
            if not item.get("company_name_normalized"):
                item["company_name_normalized"] = normalize_name(name)
            mapped.append(item)
            if raw.get(src.key_column) is not None:
                cursor = str(raw.get(src.key_column))
        if remaining is not None:
            remaining -= len(rows)
            if remaining <= 0:
                break
        if len(rows) < page:
            break
    return mapped


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
