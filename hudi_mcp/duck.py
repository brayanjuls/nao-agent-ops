"""DuckDB SQL engine.

One in-memory DuckDB database is shared by all queries and rebuilt whenever the set of tables changes. It is
locked down before any agent SQL runs:

* only the S3 prefixes / local folders of the configured Delta, Parquet, CSV and JSON tables can be read
  (`allowed_directories` / `allowed_paths` with `enable_external_access = false`), so `read_text('/etc/passwd')`,
  `read_csv('s3://another-bucket/...')`, `COPY ... TO`, `ATTACH` and `INSTALL` all fail;
* extensions are loaded up front and auto-install / auto-load is off;
* `lock_configuration = true`, so SQL cannot `SET` any of this back.

Each query gets its own cursor, where the tables it references are bound: Delta / Parquet / CSV / JSON tables
as temporary views over DuckDB's readers (DuckDB pushes columns and filters into those scans itself), Hudi
tables as Arrow tables already read through hudi-rs with the planner's pushdown.
"""

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlparse

import duckdb
import pyarrow as pa

from .catalog import GLOB_CHARS, has_glob, s3_options
from .config import Config, TableConfig

log = logging.getLogger("hudi_mcp")

_READ_ONLY_TYPES = {duckdb.StatementType.SELECT}
# Introspection of the server's own credentials, and functions that run SQL built from strings (which would
# get around the checks below).
_DENIED_FUNCTIONS = re.compile(r"\b(duckdb_secrets|which_secret|query|query_table)\s*\(", re.IGNORECASE)
_EXPLAIN = re.compile(r"^\s*explain(\s+analy[sz]e)?\b", re.IGNORECASE)


class SqlRejected(ValueError):
    pass


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_literal(value: Any) -> str:
    """Python / YAML value -> DuckDB literal, for reader options such as `header: true` or `columns: {...}`."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    if isinstance(value, dict):
        return "{" + ", ".join(f"{sql_literal(str(k))}: {sql_literal(v)}" for k, v in value.items()) + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(sql_literal(v) for v in value) + "]"
    return "'" + str(value).replace("'", "''") + "'"


def duck_uri(uri: str) -> str:
    return "s3://" + uri[len("s3a://"):] if uri.startswith("s3a://") else uri


def _glob_base(uri: str) -> str:
    """'s3://b/logs/2026-*/x.json' -> 's3://b/logs'"""
    return uri[: min(uri.index(c) for c in GLOB_CHARS if c in uri)].rsplit("/", 1)[0]


def scan_sql(t: TableConfig) -> str:
    """The DuckDB table function call that reads a Delta / Parquet / CSV / JSON table."""
    uri = duck_uri(t.uri)
    if t.format == "delta":
        return f"delta_scan({sql_literal(uri)})"
    path = f"{uri}/**/*{t.file_extension}" if t.file_extension else uri
    reader = {"parquet": "read_parquet", "csv": "read_csv", "json": "read_json"}[t.format]
    args = [sql_literal(path)]
    if t.file_extension and "hive_partitioning" not in t.read_options:
        args.append("hive_partitioning = true")
    args += [f"{k} = {sql_literal(v)}" for k, v in t.read_options.items()]
    return f"{reader}({', '.join(args)})"


def table_root(t: TableConfig) -> tuple[str, bool]:
    """(path, is_folder) DuckDB must be allowed to read for this table."""
    uri = duck_uri(t.uri)
    if has_glob(uri):
        return _glob_base(uri), True
    return uri, t.format == "delta" or bool(t.file_extension)  # plain-file folders have a file_extension


def _s3_secret_params(storage_options: dict[str, str], credential_chain: bool) -> list[str]:
    o = s3_options(storage_options)
    params = ["TYPE s3"]
    if o.get("aws_access_key_id"):
        params += [f"KEY_ID {sql_literal(o['aws_access_key_id'])}",
                   f"SECRET {sql_literal(o.get('aws_secret_access_key', ''))}"]
        if o.get("aws_session_token"):
            params.append(f"SESSION_TOKEN {sql_literal(o['aws_session_token'])}")
    elif credential_chain:
        # env vars, ~/.aws profiles and SSO, instance / task roles, IRSA. Not validated at creation, so a
        # machine without credentials can still read public buckets.
        params += ["PROVIDER credential_chain", "VALIDATION 'none'"]
    region = o.get("aws_region") or o.get("aws_default_region")
    if region:
        params.append(f"REGION {sql_literal(region)}")
    endpoint = o.get("aws_endpoint") or o.get("aws_endpoint_url")
    if endpoint:
        ep = urlparse(endpoint if "://" in endpoint else f"https://{endpoint}")
        params += [f"ENDPOINT {sql_literal(ep.netloc)}", "URL_STYLE 'path'"]
        if ep.scheme == "http":
            params.append("USE_SSL false")
    if o.get("aws_allow_http", "").lower() == "true" and "USE_SSL false" not in params:
        params.append("USE_SSL false")
    if o.get("aws_virtual_hosted_style_request", "").lower() == "false" and "URL_STYLE 'path'" not in params:
        params.append("URL_STYLE 'path'")
    return params


def check_read_only(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    """Allow exactly one SELECT / WITH / EXPLAIN SELECT statement, parsed by DuckDB itself."""
    try:
        statements = con.extract_statements(sql)
    except duckdb.Error as e:
        raise SqlRejected(f"SQL error: {e}") from e
    if len(statements) != 1:
        raise SqlRejected("Send exactly one SQL statement per call.")
    statement = statements[0]
    if statement.type == duckdb.StatementType.EXPLAIN:
        return check_read_only(con, _EXPLAIN.sub("", statement.query, count=1))
    if statement.type not in _READ_ONLY_TYPES:
        raise SqlRejected(
            f"Only read-only SELECT / WITH / EXPLAIN queries are allowed (got {statement.type.name})."
        )
    found = _DENIED_FUNCTIONS.search(sql)
    if found:
        raise SqlRejected(f"{found.group(1)}() is not allowed.")


class DuckEngine:
    def __init__(self, config: Config, storage_options):
        """`storage_options(table)` returns the merged default + table storage options."""
        self.config = config
        self._storage_options = storage_options
        self._lock = threading.Lock()
        self._db: duckdb.DuckDBPyConnection | None = None
        self._key: tuple | None = None

    # ---------- database ----------
    def _fingerprint(self, tables: list[TableConfig]) -> tuple:
        return tuple(
            sorted(
                (t.name, t.uri, t.format, t.file_extension, repr(sorted(self._storage_options(t).items())))
                for t in tables
            )
        )

    def _database(self, tables: list[TableConfig]) -> duckdb.DuckDBPyConnection:
        tables = [t for t in tables if t.format != "hudi"]  # Hudi goes through hudi-rs, not DuckDB's readers
        key = self._fingerprint(tables)
        with self._lock:
            if self._db is None or key != self._key:
                self._db, self._key = self._build(tables), key
            return self._db

    def _build(self, tables: list[TableConfig]) -> duckdb.DuckDBPyConnection:
        settings = self.config.duckdb
        config = {"autoinstall_known_extensions": "false", "autoload_known_extensions": "false"}
        if settings.memory_limit:
            config["memory_limit"] = settings.memory_limit
        if settings.threads:
            config["threads"] = settings.threads
        if settings.extension_directory:
            config["extension_directory"] = settings.extension_directory
        con = duckdb.connect(":memory:", config=config)

        s3_tables = [t for t in tables if duck_uri(t.uri).startswith("s3://")]
        needed = (["httpfs", "aws"] if s3_tables else []) + (["delta"] if any(t.format == "delta" for t in tables) else [])
        for ext in needed:
            self._load_extension(con, ext)

        if s3_tables:
            self._create_secret(con, "default_s3", self.config.default_storage_options, scope=None)
            for i, t in enumerate(s3_tables):
                if t.storage_options:  # per-table / per-discovery-prefix credentials or region
                    self._create_secret(con, f"table_{i}", self._storage_options(t), scope=table_root(t)[0])

        folders, files = set(), set()
        for t in tables:
            root, is_folder = table_root(t)
            (folders if is_folder else files).add(root + "/" if is_folder else root)
        con.execute(f"SET allowed_directories = {sql_literal(sorted(folders))}")
        con.execute(f"SET allowed_paths = {sql_literal(sorted(files))}")
        con.execute("SET enable_external_access = false")
        con.execute("SET lock_configuration = true")
        return con

    @staticmethod
    def _load_extension(con: duckdb.DuckDBPyConnection, ext: str) -> None:
        try:
            con.load_extension(ext)
        except duckdb.Error:
            try:  # not pre-installed (the Docker images pre-install them): fetch it once
                con.install_extension(ext)
                con.load_extension(ext)
            except duckdb.Error as e:
                log.warning("DuckDB extension %s is unavailable: %s", ext, e)

    @staticmethod
    def _create_secret(con, name: str, storage_options: dict[str, str], scope: str | None) -> None:
        scope_sql = [f"SCOPE {sql_literal(scope)}"] if scope else []
        try:
            params = _s3_secret_params(storage_options, credential_chain=True) + scope_sql
            con.execute(f"CREATE SECRET {name} ({', '.join(params)})")
        except duckdb.Error as e:  # e.g. the aws extension is missing: anonymous access + explicit settings
            log.warning("Could not create S3 credential-chain secret (%s); using anonymous access", e)
            params = _s3_secret_params(storage_options, credential_chain=False) + scope_sql
            con.execute(f"CREATE OR REPLACE SECRET {name} ({', '.join(params)})")

    # ---------- queries ----------
    @contextmanager
    def session(self, catalog_tables: list[TableConfig]) -> Iterator[Session]:
        """A cursor on the shared database. `catalog_tables` are all known tables (they decide the sandbox)."""
        db = self._database(catalog_tables)
        with self._lock:
            cur = db.cursor()
        try:
            yield Session(cur, self.config.limits.query_timeout_seconds)
        finally:
            cur.close()


class Session:
    def __init__(self, cur: duckdb.DuckDBPyConnection, timeout_seconds: int):
        self.cur = cur
        self.timeout_seconds = timeout_seconds

    def bind_scan(self, t: TableConfig) -> None:
        """Expose a Delta / Parquet / CSV / JSON table under its catalog name."""
        self.cur.execute(f"CREATE OR REPLACE TEMP VIEW {quote_ident(t.name)} AS SELECT * FROM {scan_sql(t)}")

    def bind_arrow(self, name: str, data: pa.Table) -> None:
        self.cur.register(name, data)

    def run(self, sql: str, max_rows: int) -> pa.Table:
        """Run checked, read-only agent SQL; at most `max_rows` rows are fetched."""
        check_read_only(self.cur, sql)
        return self.fetch(self.cur.sql(sql), max_rows)

    def fetch(self, relation: duckdb.DuckDBPyRelation, max_rows: int) -> pa.Table:
        timer = threading.Timer(self.timeout_seconds, self.cur.interrupt) if self.timeout_seconds > 0 else None
        if timer:
            timer.start()
        try:
            return relation.limit(max_rows).to_arrow_table()
        except duckdb.InterruptException as e:
            raise TimeoutError(
                f"Query exceeded {self.timeout_seconds}s and was cancelled. Filter on partition columns, "
                "select fewer columns or aggregate more."
            ) from e
        finally:
            if timer:
                timer.cancel()
