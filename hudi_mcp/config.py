"""Configuration loading for the MCP server.

Config comes from a YAML file (path in HUDI_MCP_CONFIG) and/or a quick env var
(HUDI_MCP_TABLES="name=s3://bucket/path,other=s3://bucket/other").

`${VAR}` placeholders anywhere in the YAML are replaced from the environment, so
credentials never need to live in the file.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _expand_env(value: Any) -> Any:
    """Recursively replace ${VAR} / ${VAR:-default} with environment values."""
    if isinstance(value, str):
        def repl(m: re.Match) -> str:
            name, default = m.group(1), m.group(2)
            if name in os.environ:
                return os.environ[name]
            if default is not None:
                return default
            raise ValueError(f"Environment variable '{name}' is referenced in the config but not set")
        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(v) for v in value]
    return value


def _str_dict(d: dict | None) -> dict[str, str]:
    """hudi-rs expects str -> str options; YAML may give bools/ints."""
    out: dict[str, str] = {}
    for k, v in (d or {}).items():
        if isinstance(v, bool):
            v = "true" if v else "false"
        out[str(k)] = str(v)
    return out


FORMATS = ("hudi", "delta", "parquet", "csv", "json")


def _yaml_dict(d: dict | None) -> dict[str, Any]:
    """DuckDB reader options keep their YAML types (header: true, columns: {...})."""
    return {str(k): v for k, v in (d or {}).items()}


def normalize_uri(uri: str) -> str:
    """Strip trailing slashes; make local paths absolute (DuckDB's path allow-list needs them)."""
    uri = uri.strip().rstrip("/")
    if "://" not in uri:
        return os.path.abspath(os.path.expanduser(uri))
    if uri.startswith("file://"):
        return os.path.abspath(uri[len("file://"):])
    return uri


def _format(value: Any, where: str) -> str:
    fmt = str(value or "").lower()
    if fmt and fmt not in FORMATS:
        raise ValueError(f"{where}: format must be one of {', '.join(FORMATS)}; got '{value}'")
    return fmt


def sanitize_name(raw: str) -> str:
    """Turn an arbitrary folder/table name into a safe SQL identifier."""
    name = re.sub(r"[^A-Za-z0-9_]", "_", raw.strip()).strip("_").lower()
    if not name:
        name = "table"
    if name[0].isdigit():
        name = f"t_{name}"
    return name


@dataclass
class TableConfig:
    name: str
    uri: str                # table folder, a single file, or a glob (s3://bucket/logs/*.json)
    description: str = ""
    format: str = ""        # one of FORMATS; "" = detect from the storage layout on first use
    storage_options: dict[str, str] = field(default_factory=dict)
    hudi_options: dict[str, str] = field(default_factory=dict)
    read_options: dict[str, Any] = field(default_factory=dict)  # extra DuckDB reader args (csv/json/parquet)
    file_extension: str = ""  # data file suffix inside a folder table (".parquet", ".csv.gz"...); set by detection
    source: str = "config"  # "config" or "discovered"


@dataclass
class DiscoverConfig:
    prefix: str
    max_depth: int = 3
    name_prefix: str = ""
    storage_options: dict[str, str] = field(default_factory=dict)
    formats: tuple[str, ...] = FORMATS  # which kinds of folders become tables


@dataclass
class Limits:
    max_result_rows: int = 500          # rows returned to the agent per call
    max_scan_rows: int = 5_000_000      # rows materialized in memory for time-travel / incremental
    max_cell_chars: int = 2_000         # long strings / blobs are truncated in results
    preview_rows: int = 10
    query_timeout_seconds: int = 300    # SQL running longer than this is interrupted


@dataclass
class DuckDBSettings:
    memory_limit: str = ""              # e.g. "4GB"; default: DuckDB's (80% of RAM)
    threads: int = 0                    # 0 = DuckDB default (all cores)
    extension_directory: str = ""       # where httpfs / aws / delta are installed; default ~/.duckdb/extensions


@dataclass
class Config:
    tables: list[TableConfig] = field(default_factory=list)
    discover: list[DiscoverConfig] = field(default_factory=list)
    default_storage_options: dict[str, str] = field(default_factory=dict)
    default_hudi_options: dict[str, str] = field(default_factory=dict)
    limits: Limits = field(default_factory=Limits)
    duckdb: DuckDBSettings = field(default_factory=DuckDBSettings)


def load_config(path: str | None = None) -> Config:
    path = path or os.environ.get("HUDI_MCP_CONFIG")
    raw: dict[str, Any] = {}
    if path:
        p = Path(path).expanduser()
        if not p.exists():
            raise FileNotFoundError(f"HUDI_MCP_CONFIG points to '{p}', which does not exist")
        raw = _expand_env(yaml.safe_load(p.read_text()) or {})

    defaults = raw.get("defaults") or {}
    cfg = Config(
        default_storage_options=_str_dict(defaults.get("storage_options")),
        default_hudi_options=_str_dict(defaults.get("hudi_options")),
    )

    limits = raw.get("limits") or {}
    for key in ("max_result_rows", "max_scan_rows", "max_cell_chars", "preview_rows", "query_timeout_seconds"):
        env_key = f"HUDI_MCP_{key.upper()}"
        if env_key in os.environ:
            setattr(cfg.limits, key, int(os.environ[env_key]))
        elif key in limits:
            setattr(cfg.limits, key, int(limits[key]))

    duck = raw.get("duckdb") or {}
    cfg.duckdb.memory_limit = str(os.environ.get("HUDI_MCP_DUCKDB_MEMORY_LIMIT") or duck.get("memory_limit") or "")
    cfg.duckdb.threads = int(os.environ.get("HUDI_MCP_DUCKDB_THREADS") or duck.get("threads") or 0)
    cfg.duckdb.extension_directory = str(
        os.environ.get("HUDI_MCP_DUCKDB_EXTENSION_DIRECTORY") or duck.get("extension_directory") or ""
    )

    seen: set[str] = set()

    def add_table(t: TableConfig) -> None:
        if not _NAME_PATTERN.match(t.name):
            raise ValueError(
                f"Table name '{t.name}' is not a valid SQL identifier (use letters, digits, underscore)"
            )
        if t.name in seen:
            raise ValueError(f"Duplicate table name '{t.name}' in config")
        seen.add(t.name)
        cfg.tables.append(t)

    for item in raw.get("tables") or []:
        add_table(
            TableConfig(
                name=item["name"],
                uri=normalize_uri(item["uri"]),
                description=item.get("description", ""),
                format=_format(item.get("format"), f"table '{item['name']}'"),
                storage_options=_str_dict(item.get("storage_options")),
                hudi_options=_str_dict(item.get("hudi_options")),
                read_options=_yaml_dict(item.get("read_options")),
            )
        )

    # Quick-start: HUDI_MCP_TABLES="trips=s3://bucket/trips,orders=s3://bucket/orders.parquet"
    for pair in filter(None, (os.environ.get("HUDI_MCP_TABLES") or "").split(",")):
        if "=" not in pair:
            raise ValueError(f"HUDI_MCP_TABLES entry '{pair}' must look like name=uri")
        name, uri = pair.split("=", 1)
        add_table(TableConfig(name=name.strip(), uri=normalize_uri(uri)))

    for item in raw.get("discover") or []:
        formats = tuple(_format(f, f"discover '{item['prefix']}'") for f in item.get("formats") or FORMATS)
        cfg.discover.append(
            DiscoverConfig(
                prefix=normalize_uri(item["prefix"]),
                max_depth=int(item.get("max_depth", 3)),
                name_prefix=item.get("name_prefix", ""),
                storage_options=_str_dict(item.get("storage_options")),
                formats=formats,
            )
        )
    for prefix in filter(None, (os.environ.get("HUDI_MCP_DISCOVER") or "").split(",")):
        cfg.discover.append(DiscoverConfig(prefix=normalize_uri(prefix)))

    return cfg
