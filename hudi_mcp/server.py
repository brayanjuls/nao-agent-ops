"""MCP server exposing data-lake tables in S3 (or local folders): Apache Hudi via hudi-rs, plus Delta Lake,
Parquet, CSV and JSON, all queried with DuckDB SQL.

Run:  hudi-mcp                      (stdio, for nao self-hosted / Claude / Cursor)
      hudi-mcp --transport streamable-http --host 0.0.0.0 --port 8000   (remote / nao cloud)
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Annotated, Any

import anyio
import duckdb
import pyarrow as pa
from hudi import HudiQueryType, HudiReadOptions
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

from . import __version__
from .catalog import Catalog, UnknownTableError
from .config import Config, TableConfig, load_config
from .duck import DuckEngine, Session, SqlRejected, check_read_only, duck_uri, quote_ident, sql_literal
from .formatting import human_bytes, schema_to_columns, table_to_payload
from .planner import TableScanPlan, plan_scans

log = logging.getLogger("hudi_mcp")

INSTRUCTIONS = """\
Read-only SQL access to data-lake tables in S3 (or other storage): Apache Hudi, Delta Lake, Parquet, CSV and JSON.

Suggested workflow:
1. list_tables to see which tables exist and their format.
2. describe_table for columns, partition fields and size before writing SQL.
3. query to run SQL (DuckDB dialect) against the latest data. Reference tables by the names from
   list_tables; joins across tables of any format are supported. Always filter on partition columns when you can.
4. Hudi tables only: hudi_timeline to see commits; hudi_query_as_of for time travel; hudi_query_incremental
   for records changed between two commits (CDC-style). Hudi meta columns (_hoodie_commit_time,
   _hoodie_record_key, ...) are available in every Hudi table.
"""

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=True)

_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_COMPARISONS = {"=", "!=", "<>", "<", "<=", ">", ">="}

FilterList = Annotated[
    list[list[str]] | None,
    Field(
        description=(
            'Optional pruning filters as [column, operator, value] triples, e.g. [["region","=","eu"], '
            '["dt",">=","2026-01-01"]]. Operators: =, !=, <, <=, >, >=, IN, NOT IN (IN values are '
            "comma-separated in one string). Filters on partition columns skip whole partitions and are the "
            "cheapest way to limit a read. On Hudi tables, filters on other columns only skip files, so repeat "
            "the condition in your SQL WHERE clause for exact results."
        )
    ),
]


def _normalize_ts(ts: str) -> str:
    """hudi-rs accepts Hudi instants (yyyyMMddHHmmssSSS), RFC3339 and epoch millis.
    Also accept 'YYYY-MM-DD' and 'YYYY-MM-DD HH:MM:SS' (treated as UTC)."""
    ts = ts.strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", ts):
        return f"{ts}T00:00:00Z"
    m = re.fullmatch(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}(:\d{2}(\.\d+)?)?)", ts)
    if m:
        time_part = m.group(2) if m.group(3) else f"{m.group(2)}:00"
        return f"{m.group(1)}T{time_part}Z"
    return ts


def _filters(filters: list[list[str]] | None) -> list[tuple[str, str, str]]:
    out = []
    for f in filters or []:
        if len(f) != 3:
            raise ToolError(f"Each filter must be [column, operator, value]; got {f!r}")
        out.append((str(f[0]), str(f[1]), str(f[2])))
    return out


def _where_sql(filters: list[tuple[str, str, str]]) -> str:
    """Preview filters as an exact DuckDB WHERE clause (non-Hudi tables)."""
    parts = []
    for col, op, value in filters:
        op = op.strip().upper()
        if op in _COMPARISONS:
            parts.append(f"{quote_ident(col)} {op} {sql_literal(value)}")
        elif op in ("IN", "NOT IN"):
            values = ", ".join(sql_literal(v.strip()) for v in value.split(","))
            parts.append(f"{quote_ident(col)} {op} ({values})")
        else:
            raise ToolError(f"Unsupported filter operator '{op}'; use =, !=, <, <=, >, >=, IN or NOT IN")
    return f" WHERE {' AND '.join(parts)}" if parts else ""


class LakeService:
    def __init__(self, config: Config):
        self.config = config
        self.catalog = Catalog(config)
        self.engine = DuckEngine(config, self.catalog.storage_options)

    # ---------- helpers ----------
    def _cap(self, max_rows: int | None) -> int:
        ceiling = self.config.limits.max_result_rows
        return ceiling if not max_rows or max_rows <= 0 else min(max_rows, ceiling)

    def _referenced_tables(self, sql: str) -> list[str]:
        """Names of catalog tables that appear in the SQL (string literals ignored)."""
        scrubbed = _STRING_LITERAL.sub("''", sql)
        names = []
        for name in self.catalog.all_tables():
            if re.search(rf'(?<![\w.]){re.escape(name)}(?!\w)|"{re.escape(name)}"', scrubbed, re.IGNORECASE):
                names.append(name)
        return names

    def _known_tables(self) -> list[TableConfig]:
        """Every table with its format resolved; these decide what DuckDB may read. Tables whose format
        cannot be worked out are left out here and report the reason when used."""
        out = []
        for t in self.catalog.all_tables().values():
            try:
                out.append(self.catalog.with_format(t))
            except Exception as e:
                log.warning("Table '%s' is unavailable: %s", t.name, e)
        return out

    def _hudi(self, table: str, tool: str) -> TableConfig:
        t = self.catalog.resolve(table)
        if t.format != "hudi":
            raise ToolError(f"{tool} only works on Hudi tables; '{t.name}' is a {t.format} table. Use query instead.")
        return t

    def _bind(self, session: Session, t: TableConfig) -> None:
        try:
            session.bind_scan(t)
        except duckdb.Error as e:
            raise ToolError(f"Could not open {t.format} table '{t.name}' ({t.uri}): {e}") from e

    def _run_sql(self, session: Session, sql: str, max_rows: int) -> dict:
        started = time.perf_counter()
        try:
            result = session.run(sql, max_rows + 1)
        except (SqlRejected, TimeoutError) as e:
            raise ToolError(str(e)) from e
        except duckdb.Error as e:
            msg = str(e)
            hint = ""
            if "catalog error" in msg.lower() and "table" in msg.lower():
                hint = " Use list_tables for valid table names."
            raise ToolError(f"SQL error: {msg}{hint}") from e
        payload = table_to_payload(
            result, max_rows, self.config.limits.max_cell_chars, total_rows=result.num_rows
        )
        payload["elapsed_ms"] = round((time.perf_counter() - started) * 1000)
        return payload

    def _materialize(self, table_name: str, options: HudiReadOptions, incremental: bool) -> pa.Table:
        """Read a Hudi snapshot / time-travel / incremental slice into memory, guarded by max_scan_rows."""
        t = self.catalog.table(table_name)
        limit = self.config.limits.max_scan_rows
        if incremental:  # hudi-rs has no incremental streaming yet
            batches = t.read(options)
            rows = sum(b.num_rows for b in batches)
            if rows > limit:
                raise ToolError(
                    f"Incremental read returned {rows:,} rows, above max_scan_rows={limit:,}. "
                    "Narrow the commit range or add partition filters."
                )
        else:
            batches, rows = [], 0
            for b in t.read_stream(options):
                rows += b.num_rows
                if rows > limit:
                    raise ToolError(
                        f"Snapshot read exceeds max_scan_rows={limit:,}. Add partition filters "
                        "and/or a column list to reduce the data read."
                    )
                batches.append(b)
        if batches:
            return pa.Table.from_batches(batches)
        schema = t.get_schema_with_meta_fields()
        if options.projection:
            schema = pa.schema([schema.field(c) for c in options.projection if c in schema.names])
        return schema.empty_table()

    def _query_in_memory(self, table_name: str, data: pa.Table, sql: str | None, max_rows: int) -> dict:
        name = self.catalog.resolve(table_name).name
        with self.engine.session(self._known_tables()) as session:
            session.bind_arrow(name, data)
            payload = self._run_sql(session, sql or f"SELECT * FROM {quote_ident(name)}", max_rows)
        payload["rows_read"] = data.num_rows
        return payload

    # ---------- tools ----------
    def list_tables(self, refresh: bool, include_details: bool) -> dict:
        tables = self.catalog.all_tables(refresh=refresh)

        def details(name: str) -> dict:
            cfg = tables[name]
            item: dict[str, Any] = {"name": name, "uri": cfg.uri, "source": cfg.source}
            if cfg.description:
                item["description"] = cfg.description
            try:
                item["format"] = self.catalog.with_format(cfg).format
                if include_details and cfg.format == "hudi":
                    t = self.catalog.table(name, refresh=refresh)
                    item["table_type"] = t.table_type
                    item["partition_columns"] = t.get_partition_schema().names
                    try:
                        item["latest_commit"] = t.get_timeline().get_latest_commit_timestamp()
                    except Exception:
                        item["latest_commit"] = None  # empty table
                elif include_details and cfg.format == "delta":
                    item["version"] = self.catalog.delta_version(cfg)
            except Exception as e:
                item["error"] = f"Could not open table: {e}"
            return item

        with ThreadPoolExecutor(max_workers=8) as pool:
            items = list(pool.map(details, sorted(tables)))
        return {"tables": items, "count": len(items)}

    def describe_table(self, table: str, include_storage_stats: bool, include_partitions: bool) -> dict:
        cfg = self.catalog.resolve(table)
        if cfg.format == "hudi":
            return self._describe_hudi(cfg, include_storage_stats, include_partitions)
        out: dict[str, Any] = {
            "name": cfg.name,
            "format": cfg.format,
            "uri": cfg.uri,
            "description": cfg.description or None,
        }
        with self.engine.session(self._known_tables()) as session:
            self._bind(session, cfg)
            described = session.cur.sql(f"DESCRIBE SELECT * FROM {quote_ident(cfg.name)}").fetchall()
            out["columns"] = [{"name": r[0], "type": r[1], "nullable": r[2] != "NO"} for r in described]
            if cfg.format == "delta":
                out.update(self._delta_layout(session, cfg, include_storage_stats, include_partitions))
            else:
                out.update(self._files_layout(session, cfg, include_storage_stats, include_partitions))
        return out

    def _delta_layout(self, session: Session, cfg: TableConfig, stats: bool, partitions: bool) -> dict:
        files = session.cur.sql(
            f"SELECT partitions, cardinality FROM delta_list_files({sql_literal(duck_uri(cfg.uri))})"
        ).fetchall()
        paths = sorted({"/".join(f"{k}={v}" for k, v in (p or {}).items()) for p, _ in files} - {""})
        out: dict[str, Any] = {
            "version": self.catalog.delta_version(cfg),
            "partition_fields": list((files[0][0] or {}).keys()) if files else [],
        }
        if stats:
            rows = [c for _, c in files]
            out["storage"] = {
                "files": len(files),
                "partitions": len(paths),
                "estimated_rows": sum(rows) if rows and None not in rows else None,
            }
            if partitions:
                out["storage"]["partition_paths"] = paths[:500]
                if len(paths) > 500:
                    out["storage"]["partition_paths_truncated"] = True
        return out

    def _files_layout(self, session: Session, cfg: TableConfig, stats: bool, partitions: bool) -> dict:
        files = self.catalog.data_files(cfg) if (stats or cfg.file_extension) else []
        dirs = sorted({rel.rsplit("/", 1)[0] for rel, _ in files if "/" in rel})
        hive = [d for d in dirs if all("=" in part for part in d.split("/"))]
        out: dict[str, Any] = {
            "partition_fields": [p.split("=", 1)[0] for p in hive[0].split("/")] if hive else [],
        }
        if cfg.read_options:
            out["read_options"] = cfg.read_options
        if stats:
            total = sum(size for _, size in files)
            out["storage"] = {
                "files": len(files),
                "partitions": len(hive),
                "total_bytes": total,
                "total_size": human_bytes(total),
            }
            if cfg.format == "parquet" and 0 < len(files) <= 1000:  # row counts from the parquet footers
                path = f"{duck_uri(cfg.uri)}/**/*{cfg.file_extension}" if cfg.file_extension else duck_uri(cfg.uri)
                out["storage"]["rows"] = session.cur.sql(
                    f"SELECT sum(num_rows)::BIGINT FROM parquet_file_metadata({sql_literal(path)})"
                ).fetchone()[0]
            if partitions:
                out["storage"]["partition_paths"] = hive[:500]
                if len(hive) > 500:
                    out["storage"]["partition_paths_truncated"] = True
        return out

    def _describe_hudi(self, cfg: TableConfig, include_storage_stats: bool, include_partitions: bool) -> dict:
        t = self.catalog.table(cfg.name)
        props = t.hudi_options()

        def prop(*keys: str) -> str | None:
            for k in keys:
                if props.get(k):
                    return props[k]
            return None

        tl = t.get_timeline()
        instants = tl.get_completed_commits() + tl.get_completed_deltacommits() + tl.get_completed_replacecommits()
        instants.sort(key=lambda i: i.timestamp)

        out: dict[str, Any] = {
            "name": cfg.name,
            "format": "hudi",
            "hudi_table_name": t.table_name,
            "uri": cfg.uri,
            "description": cfg.description or None,
            "table_type": t.table_type,
            "record_key_fields": [f for f in (prop("hoodie.table.recordkey.fields") or "").split(",") if f] or None,
            "partition_fields": t.get_partition_schema().names,
            "ordering_fields": prop("hoodie.table.ordering.fields", "hoodie.table.precombine.field"),
            "hive_style_partitioning": prop("hoodie.datasource.write.hive_style_partitioning"),
            "base_file_format": prop("hoodie.table.base.file.format"),
            "table_version": prop("hoodie.table.version"),
            "timezone": t.timezone,
            "columns": schema_to_columns(t.get_schema()),
            "meta_columns": [f.name for f in t.get_schema_with_meta_fields() if f.name.startswith("_hoodie_")],
            "commits": {
                "count": len(instants),
                "first": instants[0].timestamp if instants else None,
                "latest": instants[-1].timestamp if instants else None,
            },
        }
        if include_storage_stats:
            slices = t.get_file_slices()
            total = sum(s.total_size_bytes() for s in slices)
            partitions = sorted({s.partition_path for s in slices})
            out["storage"] = {
                "file_groups": len(slices),
                "partitions": len(partitions),
                "total_bytes": total,
                "total_size": human_bytes(total),
                "file_groups_with_log_files": sum(1 for s in slices if s.has_log_files()),
            }
            stats = t.compute_table_stats()
            if stats:
                out["storage"]["estimated_rows"], out["storage"]["estimated_bytes"] = stats
            if include_partitions:
                out["storage"]["partition_paths"] = partitions[:500]
                if len(partitions) > 500:
                    out["storage"]["partition_paths_truncated"] = True
        return out

    def preview_table(self, table: str, limit: int, columns: list[str] | None, filters) -> dict:
        cfg = self.catalog.resolve(table)
        limit = max(1, min(limit or self.config.limits.preview_rows, self.config.limits.max_result_rows))
        if cfg.format != "hudi":
            select = ", ".join(quote_ident(c) for c in columns) if columns else "*"
            sql = f"SELECT {select} FROM {quote_ident(cfg.name)}{_where_sql(_filters(filters))}"
            with self.engine.session(self._known_tables()) as session:
                self._bind(session, cfg)
                try:
                    data = session.fetch(session.cur.sql(sql), limit)
                except duckdb.Error as e:
                    raise ToolError(f"SQL error: {e}") from e
            return table_to_payload(data, limit, self.config.limits.max_cell_chars)

        t = self.catalog.table(table)
        opts = HudiReadOptions(filters=_filters(filters) or None, projection=columns or None).with_batch_size(
            max(limit, 64)
        )
        batches, rows = [], 0
        for b in t.read_stream(opts):  # stop as soon as we have enough rows
            batches.append(b)
            rows += b.num_rows
            if rows >= limit:
                break
        data = pa.Table.from_batches(batches) if batches else t.get_schema_with_meta_fields().empty_table()
        return table_to_payload(data, limit, self.config.limits.max_cell_chars, total_rows=min(rows, limit))

    def timeline(self, table: str, limit: int, include_stats: bool) -> dict:
        self._hudi(table, "hudi_timeline")
        t = self.catalog.table(table)
        tl = t.get_timeline()
        instants = tl.get_completed_commits() + tl.get_completed_deltacommits() + tl.get_completed_replacecommits()
        instants.sort(key=lambda i: i.timestamp, reverse=True)
        total = len(instants)
        items = []
        for inst in instants[: max(1, limit)]:
            item: dict[str, Any] = {
                "timestamp": inst.timestamp,
                "action": inst.action,
                "completed_at_epoch_ms": inst.epoch_mills,
            }
            if include_stats:
                try:
                    meta = json.loads(tl.get_instant_metadata_in_json(inst))
                    stats = [s for lst in (meta.get("partitionToWriteStats") or {}).values() for s in (lst or [])]
                    item.update(
                        operation=meta.get("operationType"),
                        partitions_written=len(meta.get("partitionToWriteStats") or {}),
                        files_written=len(stats),
                        records_written=sum(s.get("numWrites", 0) or 0 for s in stats),
                        inserts=sum(s.get("numInserts", 0) or 0 for s in stats),
                        updates=sum(s.get("numUpdateWrites", 0) or 0 for s in stats),
                        deletes=sum(s.get("numDeletes", 0) or 0 for s in stats),
                        bytes_written=sum(s.get("totalWriteBytes", 0) or 0 for s in stats),
                    )
                except Exception as e:
                    item["stats_error"] = str(e)
            items.append(item)
        return {"table": table, "total_commits": total, "commits": items, "order": "newest first"}

    def query(self, sql: str, max_rows: int | None) -> dict:
        names = self._referenced_tables(sql)
        if not names:
            known = ", ".join(sorted(self.catalog.all_tables())) or "(none)"
            raise ToolError(f"The SQL does not reference any known table. Known tables: {known}")
        used = [self.catalog.resolve(n) for n in names]
        hudi = [t for t in used if t.format == "hudi"]
        max_rows = self._cap(max_rows)
        with self.engine.session(self._known_tables()) as session:
            try:
                check_read_only(session.cur, sql)  # before reading anything
            except SqlRejected as e:
                raise ToolError(str(e)) from e
            for t in used:
                if t.format != "hudi":
                    self._bind(session, t)
            pushdown = self._load_hudi(session, sql, hudi)
            payload = self._run_sql_with_retry(session, sql, max_rows, hudi, pushdown)
        payload["tables_used"] = {t.name: t.format for t in used}
        payload["engine"] = "duckdb"
        return payload

    def _run_sql_with_retry(self, session: Session, sql: str, max_rows: int, hudi, pushdown: dict) -> dict:
        try:
            payload = self._run_sql(session, sql, max_rows)
        except ToolError as e:
            # The planner under-estimated the columns a Hudi table needs: re-read it with every column, once.
            narrowed = any(p["columns_read"] != "all" for p in pushdown.values())
            if not narrowed or not re.search(r"field|column|schema", str(e), re.IGNORECASE):
                raise
            pushdown = self._load_hudi(session, sql, hudi, full_projection=True)
            payload = self._run_sql(session, sql, max_rows)
        if pushdown:
            payload["pushdown"] = pushdown
        return payload

    def _load_hudi(
        self, session: Session, sql: str, tables: list[TableConfig], full_projection: bool = False
    ) -> dict:
        """Read each Hudi table through hudi-rs with the planner's columns / filters and register it in DuckDB."""
        if not tables:
            return {}
        schemas = {t.name.lower(): self.catalog.table(t.name).get_schema_with_meta_fields().names for t in tables}
        try:
            plans = plan_scans(sql, schemas)
        except Exception as e:  # unparseable for sqlglot: DuckDB may still accept it
            log.info("SQL analysis failed (%s); reading full tables", e)
            plans = {}
        pushdown = {}
        for t in tables:
            plan = plans.get(t.name.lower()) or TableScanPlan(name=t.name.lower())
            columns = None if full_projection else plan.columns
            filters = plan.filters
            try:
                data = self._materialize(
                    t.name, HudiReadOptions(filters=filters or None, projection=columns), incremental=False
                )
            except ToolError:
                raise
            except Exception as e:
                if not filters:
                    raise
                log.info("Filter pushdown rejected for %s (%s); retrying without filters", t.name, e)
                filters = []
                data = self._materialize(t.name, HudiReadOptions(projection=columns), incremental=False)
            session.bind_arrow(t.name, data)
            pushdown[t.name] = {
                "columns_read": columns if columns is not None else "all",
                "filters_pushed": [list(f) for f in filters],
                "rows_read": data.num_rows,
            }
        return pushdown

    def query_as_of(self, table, as_of_timestamp, sql, columns, filters, max_rows) -> dict:
        self._hudi(table, "hudi_query_as_of")
        opts = HudiReadOptions(filters=_filters(filters) or None, projection=columns or None).with_as_of_timestamp(
            _normalize_ts(as_of_timestamp)
        )
        data = self._materialize(table, opts, incremental=False)
        payload = self._query_in_memory(table, data, sql, self._cap(max_rows))
        payload["as_of_timestamp"] = as_of_timestamp
        return payload

    def query_incremental(self, table, start_timestamp, end_timestamp, sql, columns, filters, max_rows) -> dict:
        self._hudi(table, "hudi_query_incremental")
        opts = HudiReadOptions(filters=_filters(filters) or None, projection=columns or None)
        opts = opts.with_query_type(HudiQueryType.Incremental).with_start_timestamp(_normalize_ts(start_timestamp))
        if end_timestamp:
            opts = opts.with_end_timestamp(_normalize_ts(end_timestamp))
        data = self._materialize(table, opts, incremental=True)
        payload = self._query_in_memory(table, data, sql, self._cap(max_rows))
        payload["range"] = {"start_exclusive": start_timestamp, "end_inclusive": end_timestamp or "latest"}
        return payload


def build_server(config: Config | None = None) -> MCPServer:
    service = LakeService(config or load_config())
    mcp = MCPServer(
        name="s3-query", title="S3 data lake (Hudi, Delta, Parquet, CSV, JSON)", instructions=INSTRUCTIONS,
        version=__version__,
    )

    async def run(fn, *args):
        try:
            return await anyio.to_thread.run_sync(lambda: fn(*args))
        except ToolError:
            raise
        except UnknownTableError as e:
            raise ToolError(str(e.args[0])) from e
        except Exception as e:
            log.exception("Tool failed")
            raise ToolError(f"{type(e).__name__}: {e}") from e

    @mcp.tool(annotations=READ_ONLY)
    async def list_tables(
        refresh: Annotated[bool, Field(description="Re-scan discovery prefixes and reload table metadata.")] = False,
        include_details: Annotated[
            bool,
            Field(description="Open Hudi / Delta tables to report type, partition columns and latest commit/version."),
        ] = True,
    ) -> dict:
        """List the tables this server can read (configured and auto-discovered in S3) with URI and format
        (hudi, delta, parquet, csv, json). Hudi tables also report table type, partition columns and latest
        commit; Delta tables their version. Start here."""
        return await run(service.list_tables, refresh, include_details)

    @mcp.tool(annotations=READ_ONLY)
    async def describe_table(
        table: Annotated[str, Field(description="Table name from list_tables.")],
        include_storage_stats: Annotated[
            bool, Field(description="List the table's files to report size, row, partition and file counts.")
        ] = True,
        include_partitions: Annotated[bool, Field(description="Also return the partition paths (max 500).")] = False,
    ) -> dict:
        """Describe a table: columns and types, partition fields and storage size, plus record key, table type
        and commit range for Hudi or version for Delta. Call before writing SQL against a table."""
        return await run(service.describe_table, table, include_storage_stats, include_partitions)

    @mcp.tool(annotations=READ_ONLY)
    async def preview_table(
        table: Annotated[str, Field(description="Table name from list_tables.")],
        limit: Annotated[int, Field(description="Number of rows to return.", ge=1)] = 10,
        columns: Annotated[list[str] | None, Field(description="Columns to return (default: all).")] = None,
        filters: FilterList = None,
    ) -> dict:
        """Return sample rows from the latest data of a table. Reads only as much data as needed."""
        return await run(service.preview_table, table, limit, columns, filters)

    @mcp.tool(annotations=READ_ONLY)
    async def query(
        sql: Annotated[
            str,
            Field(
                description=(
                    "One read-only SQL statement (SELECT / WITH / EXPLAIN) in DuckDB dialect. Reference tables "
                    "by their list_tables names; joins across tables of any format are supported. Filters on "
                    "partition columns and the selected columns are pushed down into the table scans."
                )
            ),
        ],
        max_rows: Annotated[int | None, Field(description="Max rows to return (capped by server config).")] = None,
    ) -> dict:
        """Run SQL against the latest data of one or more tables (Hudi merge-on-read tables are merged and
        Delta deletion vectors applied, so results reflect the latest upserts and deletes). Prefer
        aggregations over pulling raw rows."""
        return await run(service.query, sql, max_rows)

    @mcp.tool(annotations=READ_ONLY)
    async def hudi_timeline(
        table: Annotated[str, Field(description="Hudi table name from list_tables.")],
        limit: Annotated[int, Field(description="Number of most recent commits to return.", ge=1)] = 20,
        include_stats: Annotated[
            bool, Field(description="Include operation type and inserted/updated/deleted record counts.")
        ] = True,
    ) -> dict:
        """List completed commits (commit, deltacommit, replacecommit) of a Hudi table, newest first, with the
        write operation and record counts. Use the timestamps with hudi_query_as_of / hudi_query_incremental."""
        return await run(service.timeline, table, limit, include_stats)

    @mcp.tool(annotations=READ_ONLY)
    async def hudi_query_as_of(
        table: Annotated[str, Field(description="Hudi table name from list_tables.")],
        as_of_timestamp: Annotated[
            str,
            Field(
                description=(
                    "Point in time: a Hudi instant (20260307162434124), RFC3339 (2026-03-07T16:24:35Z), "
                    "'YYYY-MM-DD[ HH:MM:SS]' (UTC) or epoch millis."
                )
            ),
        ],
        sql: Annotated[
            str | None,
            Field(description="SQL over the table as it was at that time; refer to it by its table name. "
                  "Default: SELECT * FROM <table>."),
        ] = None,
        columns: Annotated[list[str] | None, Field(description="Columns to read (reduces memory).")] = None,
        filters: FilterList = None,
        max_rows: Annotated[int | None, Field(description="Max rows to return (capped by server config).")] = None,
    ) -> dict:
        """Time travel: query a Hudi table as it was at a past commit/time, e.g. to compare a metric before and
        after a load. The snapshot is read into memory, so use filters/columns on large tables."""
        return await run(service.query_as_of, table, as_of_timestamp, sql, columns, filters, max_rows)

    @mcp.tool(annotations=READ_ONLY)
    async def hudi_query_incremental(
        table: Annotated[str, Field(description="Hudi table name from list_tables.")],
        start_timestamp: Annotated[
            str, Field(description="Exclusive start (Hudi instant, RFC3339, 'YYYY-MM-DD[ HH:MM:SS]' UTC or epoch ms).")
        ],
        end_timestamp: Annotated[str | None, Field(description="Inclusive end; default latest commit.")] = None,
        sql: Annotated[
            str | None,
            Field(description="SQL over the changed records, referring to the table by name. "
                  "Default: SELECT * FROM <table>."),
        ] = None,
        columns: Annotated[list[str] | None, Field(description="Columns to read.")] = None,
        filters: FilterList = None,
        max_rows: Annotated[int | None, Field(description="Max rows to return (capped by server config).")] = None,
    ) -> dict:
        """Incremental (CDC-style) query: records inserted or updated by commits in (start, end]. Each row keeps
        _hoodie_commit_time so you can group changes by commit."""
        return await run(
            service.query_incremental, table, start_timestamp, end_timestamp, sql, columns, filters, max_rows
        )

    return mcp


def main() -> None:
    parser = argparse.ArgumentParser(description="MCP server for Hudi, Delta, Parquet, CSV and JSON tables in S3")
    parser.add_argument("--transport", choices=["stdio", "streamable-http", "sse"], default="stdio")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--config", help="Path to YAML config (default: $HUDI_MCP_CONFIG)")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    # stdout belongs to the MCP protocol on stdio; logs go to stderr.
    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    server = build_server(load_config(args.config))
    if args.transport == "stdio":
        server.run("stdio")
    else:
        server.run(args.transport, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
