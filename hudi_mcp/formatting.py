"""Turn Arrow results into compact, JSON-safe payloads for the agent."""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import math
import uuid
from typing import Any

import pyarrow as pa


def _cell(v: Any, max_chars: int) -> Any:
    if v is None or isinstance(v, (bool, int)):
        return v
    if isinstance(v, float):
        return None if math.isnan(v) or math.isinf(v) else v
    if isinstance(v, str):
        return v if len(v) <= max_chars else v[:max_chars] + f"…[+{len(v) - max_chars} chars]"
    if isinstance(v, decimal.Decimal):
        return str(v)  # keep exact precision
    if isinstance(v, (dt.datetime, dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, dt.timedelta):
        return v.total_seconds()
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        b = bytes(v)
        enc = base64.b64encode(b[:max_chars]).decode()
        return {"base64": enc, "bytes": len(b), "truncated": len(b) > max_chars}
    if isinstance(v, dict):
        return {str(k): _cell(x, max_chars) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        if v and isinstance(v[0], tuple) and len(v[0]) == 2:  # arrow map -> list of pairs
            try:
                return {str(k): _cell(x, max_chars) for k, x in v}
            except Exception:
                pass
        return [_cell(x, max_chars) for x in v]
    return str(v)


def schema_to_columns(schema: pa.Schema) -> list[dict[str, Any]]:
    return [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in schema]


def table_to_payload(table: pa.Table, max_rows: int, max_chars: int, total_rows: int | None = None) -> dict:
    """Rows as records. `truncated` tells the agent to aggregate or filter instead."""
    total = table.num_rows if total_rows is None else total_rows
    shown = table.slice(0, max_rows)
    rows = [{k: _cell(v, max_chars) for k, v in r.items()} for r in shown.to_pylist()]
    payload: dict[str, Any] = {
        "columns": schema_to_columns(table.schema),
        "rows": rows,
        "row_count": len(rows),
        "truncated": total > len(rows),
    }
    if payload["truncated"]:
        payload["note"] = (
            f"Result has more than {len(rows)} rows; only the first {len(rows)} are shown. "
            "Aggregate, filter, or add LIMIT to get a complete answer."
        )
    return payload


def human_bytes(n: int | None) -> str | None:
    if n is None:
        return None
    size = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024
    return None
