"""Static analysis of agent SQL to push work down into hudi-rs reads.

hudi-rs's reader takes a column projection and (field, op, value) filters, and
prunes partitions/files with them. We extract, per referenced Hudi table:

* the columns the query needs (all columns when it uses `*` or anything ambiguous), and
* simple `column <op> literal` conjuncts from WHERE clauses.

This is only an optimization: DuckDB still executes the full SQL on what was
read, so every filter is re-applied exactly. Pushed filters must therefore only
ever *remove rows that cannot match* — we push a conjunct only when the table is
referenced once in the whole query and the conjunct sits in the WHERE of the
SELECT that reads it (never ON clauses, never inside OR / NOT).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

_OPS = {exp.EQ: "=", exp.NEQ: "!=", exp.GT: ">", exp.GTE: ">=", exp.LT: "<", exp.LTE: "<="}
_FLIP = {"=": "=", "!=": "!=", ">": "<", ">=": "<=", "<": ">", "<=": ">="}


@dataclass
class TableScanPlan:
    name: str                              # catalog table name
    columns: list[str] | None = None       # None = all columns
    filters: list[tuple[str, str, str]] = field(default_factory=list)


def _literal_value(node: exp.Expression) -> str | None:
    """'eu' / 42 / DATE '2026-01-01' / -3 -> string; anything else -> None."""
    if isinstance(node, exp.Literal):
        return str(node.this)
    if isinstance(node, exp.Cast) and isinstance(node.this, exp.Literal):
        return str(node.this.this)
    if isinstance(node, exp.Neg) and isinstance(node.this, exp.Literal) and not node.this.is_string:
        return f"-{node.this.this}"
    return None


def _conjuncts(node: exp.Expression):
    node = node.unnest() if isinstance(node, exp.Paren) else node
    if isinstance(node, exp.And):
        yield from _conjuncts(node.left)
        yield from _conjuncts(node.right)
    else:
        yield node


def plan_scans(sql: str, schemas: dict[str, list[str]]) -> dict[str, TableScanPlan]:
    """`schemas` maps lower-cased catalog table name -> column names (incl. meta fields).

    Returns a plan per referenced catalog table. Raises sqlglot.ParseError on SQL
    it cannot parse; callers should then fall back to full scans.
    """
    tree = sqlglot.parse_one(sql, read="duckdb")
    cte_names = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}

    refs: dict[str, list[exp.Table]] = {}
    for t in tree.find_all(exp.Table):
        key = t.name.lower()
        if key in schemas and key not in cte_names and not t.args.get("db"):
            refs.setdefault(key, []).append(t)

    plans: dict[str, TableScanPlan] = {}
    for key, tables in refs.items():
        cols = {c.lower(): c for c in schemas[key]}
        aliases = {t.alias_or_name.lower() for t in tables} | {key}
        plan = TableScanPlan(name=key)

        # ---- projection ----
        needed: set[str] | None = set()
        for star in tree.find_all(exp.Star):
            parent = star.parent
            qualifier = parent.table.lower() if isinstance(parent, exp.Column) and parent.table else ""
            if isinstance(parent, exp.Count) or (isinstance(parent, exp.Column) and qualifier not in aliases and qualifier):
                continue  # COUNT(*) or other_table.*
            needed = None  # SELECT * / t.*
            break
        if needed is not None:
            for c in tree.find_all(exp.Column):
                q = c.table.lower() if c.table else ""
                if (not q or q in aliases or q not in {t.alias_or_name.lower() for ts in refs.values() for t in ts}):
                    if c.name.lower() in cols:
                        needed.add(cols[c.name.lower()])
            if not needed:  # e.g. SELECT COUNT(*) FROM t: read the narrowest thing available
                needed = {schemas[key][0]}
        plan.columns = sorted(needed) if needed is not None else None

        # ---- filters (only when the table is read in exactly one place) ----
        # Comparison / IN / BETWEEN predicates are null-rejecting, so pushing them from the WHERE of the
        # SELECT that reads the table is safe for inner and outer joins alike.
        if len(tables) == 1:
            select = tables[0].find_ancestor(exp.Select)
            where = select.args.get("where") if select else None
            if where is not None:
                tables_in_scope = [
                    t for t in select.find_all(exp.Table) if t.find_ancestor(exp.Select) is select
                ]
                single = len(tables_in_scope) == 1
                for cond in _conjuncts(where.this):
                    plan.filters.extend(_filters_from(cond, cols, aliases, single))
        plans[key] = plan
    return plans


def _column_for(node: exp.Expression, cols: dict[str, str], aliases: set[str], single: bool) -> str | None:
    if not isinstance(node, exp.Column):
        return None
    q = node.table.lower() if node.table else ""
    if q and q not in aliases:
        return None
    if not q and not single:  # unqualified column in a multi-table SELECT: can't be sure it's ours
        return None
    return cols.get(node.name.lower())


def _filters_from(cond: exp.Expression, cols, aliases, single) -> list[tuple[str, str, str]]:
    for klass, op in _OPS.items():
        if isinstance(cond, klass):
            col, val = _column_for(cond.left, cols, aliases, single), _literal_value(cond.right)
            if col and val is not None:
                return [(col, op, val)]
            col, val = _column_for(cond.right, cols, aliases, single), _literal_value(cond.left)
            if col and val is not None:
                return [(col, _FLIP[op], val)]
            return []
    if isinstance(cond, exp.In) and not cond.args.get("query"):
        col = _column_for(cond.this, cols, aliases, single)
        vals = [_literal_value(e) for e in cond.expressions]
        if col and vals and all(v is not None for v in vals) and not any("," in v for v in vals):
            return [(col, "IN", ",".join(vals))]
    if isinstance(cond, exp.Between):
        col = _column_for(cond.this, cols, aliases, single)
        lo, hi = _literal_value(cond.args["low"]), _literal_value(cond.args["high"])
        if col and lo is not None and hi is not None:
            return [(col, ">=", lo), (col, "<=", hi)]
    return []
