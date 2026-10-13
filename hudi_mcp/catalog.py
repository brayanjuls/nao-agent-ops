"""Table catalog: resolves configured + discovered tables (Hudi, Delta, Parquet, CSV, JSON) and caches
Hudi handles."""

from __future__ import annotations

import fnmatch
import logging
import os
import re
import threading
import time
from collections import Counter
from dataclasses import dataclass
from urllib.parse import urlparse

from hudi import HudiTable, HudiTableBuilder

from .config import Config, DiscoverConfig, TableConfig, sanitize_name

log = logging.getLogger("hudi_mcp")

TABLE_CACHE_SECONDS = float(os.environ.get("HUDI_MCP_TABLE_CACHE_SECONDS", "60"))
DISCOVERY_CACHE_SECONDS = float(os.environ.get("HUDI_MCP_DISCOVERY_CACHE_SECONDS", "300"))


class UnknownTableError(KeyError):
    pass


GLOB_CHARS = "*?["
_EXTENSIONS = {
    ".parquet": "parquet", ".pq": "parquet",
    ".csv": "csv", ".tsv": "csv",
    ".json": "json", ".jsonl": "json", ".ndjson": "json",
}
_COMPRESSIONS = ("", ".gz", ".zst")  # DuckDB decompresses these for CSV / JSON


def data_file_format(name: str) -> tuple[str, str] | None:
    """'part-0.snappy.parquet' -> ('parquet', '.parquet'); 'a.csv.gz' -> ('csv', '.csv.gz'); else None."""
    lower = name.lower()
    if lower.startswith((".", "_")):  # _SUCCESS, .crc, ...
        return None
    for ext, fmt in _EXTENSIONS.items():
        for comp in _COMPRESSIONS if fmt != "parquet" else ("",):
            if lower.endswith(ext + comp):
                return fmt, ext + comp
    return None


def has_glob(uri: str) -> bool:
    return any(c in uri for c in GLOB_CHARS)


_CREDENTIAL_KEYS = ("aws_access_key_id", "aws_secret_access_key", "aws_session_token", "aws_profile")


def s3_options(storage_options: dict[str, str]) -> dict[str, str]:
    """AWS_* environment variables overlaid with the storage options, keys lower-cased.

    Accepts the same keys you give hudi-rs (aws_region, aws_endpoint, ...), in any case. When the options
    carry their own access key, no credential comes from the environment: an AWS_SESSION_TOKEN meant for
    another account must not be sent along with them."""
    opts = {k.lower(): v for k, v in storage_options.items()}
    env = {k.lower(): v for k, v in os.environ.items() if k.upper().startswith("AWS_")}
    if opts.get("aws_access_key_id"):
        env = {k: v for k, v in env.items() if k not in _CREDENTIAL_KEYS}
    return {**env, **opts}


def _pyarrow_fs(uri: str, storage_options: dict[str, str]):
    """Build a pyarrow filesystem for directory listing (discovery, format detection, storage stats).

    Missing credentials fall back to the AWS default chain (env vars, ~/.aws profile, IAM role / IRSA).
    """
    from pyarrow import fs as pafs

    parsed = urlparse(uri)
    if parsed.scheme in ("s3", "s3a"):
        merged = s3_options(storage_options)
        kwargs = {}
        if "aws_access_key_id" in merged:
            kwargs["access_key"] = merged["aws_access_key_id"]
            kwargs["secret_key"] = merged.get("aws_secret_access_key")
            if merged.get("aws_session_token"):
                kwargs["session_token"] = merged["aws_session_token"]
        region = merged.get("aws_region") or merged.get("aws_default_region")
        if region:
            kwargs["region"] = region
        endpoint = merged.get("aws_endpoint") or merged.get("aws_endpoint_url")
        if endpoint:
            ep = urlparse(endpoint if "://" in endpoint else f"https://{endpoint}")
            kwargs["endpoint_override"] = ep.netloc
            kwargs["scheme"] = ep.scheme or "https"
        if merged.get("aws_allow_http", "").lower() == "true":
            kwargs["scheme"] = "http"
        if merged.get("aws_virtual_hosted_style_request", "").lower() == "false" or endpoint:
            kwargs["force_virtual_addressing"] = False
        return pafs.S3FileSystem(**kwargs), f"{parsed.netloc}{parsed.path}".rstrip("/")
    if parsed.scheme in ("", "file"):
        path = parsed.path if parsed.scheme == "file" else uri
        return pafs.LocalFileSystem(), os.path.abspath(os.path.expanduser(path))
    # gs://, abfs://, ... : let pyarrow figure it out
    filesystem, path = pafs.FileSystem.from_uri(uri)
    return filesystem, path.rstrip("/")


def _join_uri(prefix_uri: str, rel: str) -> str:
    return f"{prefix_uri.rstrip('/')}/{rel}" if rel else prefix_uri


def _is_dir(info) -> bool:
    from pyarrow import fs as pafs

    return info.type == pafs.FileType.Directory


def _list(filesystem, path: str) -> list:
    from pyarrow import fs as pafs

    return filesystem.get_file_info(pafs.FileSelector(path, allow_not_found=True))


def classify_folder(filesystem, path: str, children: list | None = None, depth: int = 0) -> tuple[str, str] | None:
    """(format, file_extension) when the folder holds a table, else None.

    Hudi: `.hoodie/hoodie.properties`. Delta: `_delta_log/`. Plain files: data files directly in the folder
    (and no other sub-folders), or hive-style `key=value` partition folders that lead to data files.
    """
    from pyarrow import fs as pafs

    children = _list(filesystem, path) if children is None else children
    dirs = {c.base_name for c in children if _is_dir(c)}
    if ".hoodie" in dirs:
        info = filesystem.get_file_info(f"{path}/.hoodie/hoodie.properties")
        if info.type == pafs.FileType.File:
            return "hudi", ""
    if "_delta_log" in dirs:
        return "delta", ""
    partitions = sorted(d for d in dirs if "=" in d and not d.startswith((".", "_")))
    others = [d for d in dirs if "=" not in d and not d.startswith((".", "_"))]
    if others:
        return None  # a container of other folders, not a table
    found = Counter(f for c in children if not _is_dir(c) and (f := data_file_format(c.base_name)))
    if found:
        return found.most_common(1)[0][0]
    if partitions and depth < 8:  # dt=.../hour=.../part-0.parquet
        return classify_folder(filesystem, f"{path}/{partitions[0]}", depth=depth + 1)
    return None


DEFAULT_EXTENSIONS = {"parquet": ".parquet", "csv": ".csv", "json": ".json"}


def inspect_layout(uri: str, storage_options: dict[str, str]) -> tuple[str | None, str, bool]:
    """(format or None if unknown, data file extension, is_folder) of a table URI."""
    from pyarrow import fs as pafs

    leaf = uri.rsplit("/", 1)[-1]
    if has_glob(uri):
        found = data_file_format(re.sub(r"[*?\[\]]", "x", leaf))
        return (found[0] if found else None), "", False
    filesystem, path = _pyarrow_fs(uri, storage_options)
    info = filesystem.get_file_info(path)
    if info.type == pafs.FileType.File:
        found = data_file_format(leaf)
        return (found[0] if found else None), "", False
    if info.type == pafs.FileType.Directory:
        fmt, ext = classify_folder(filesystem, path) or (None, "")
        return fmt, ext, True
    raise ValueError(f"'{uri}' does not exist (or the credentials cannot list it)")


def discover_tables(d: DiscoverConfig, default_storage: dict[str, str]) -> list[TableConfig]:
    """Walk a prefix and return every folder that holds a table of one of `d.formats`."""
    storage = {**default_storage, **d.storage_options}
    filesystem, root = _pyarrow_fs(d.prefix, storage)
    found: list[TableConfig] = []

    def walk(path: str, rel: str, depth: int) -> None:
        try:
            children = _list(filesystem, path)
        except OSError as e:
            log.warning("Discovery could not list %s: %s", path, e)
            return
        kind = classify_folder(filesystem, path, children)
        if kind:
            fmt, ext = kind
            if fmt in d.formats:
                leaf = rel.rsplit("/", 1)[-1] if rel else os.path.basename(root)
                found.append(
                    TableConfig(
                        name=sanitize_name(f"{d.name_prefix}{leaf}"),
                        uri=_join_uri(d.prefix, rel),
                        format=fmt,
                        file_extension=ext,
                        storage_options=dict(d.storage_options),
                        source="discovered",
                    )
                )
            return  # never descend into a table (partitions, .hoodie, _delta_log, ...)
        if depth >= d.max_depth:
            return
        for child in children:
            name = child.base_name
            if not _is_dir(child) or name.startswith((".", "_")) or "=" in name:
                continue
            walk(child.path, f"{rel}/{name}" if rel else name, depth + 1)

    walk(root, "", 0)
    return found


@dataclass
class _Cached:
    value: object
    at: float


class Catalog:
    def __init__(self, config: Config):
        self.config = config
        self._lock = threading.Lock()
        self._tables: dict[str, _Cached] = {}
        self._discovered: _Cached | None = None
        self._formats: dict[str, tuple[str | None, str, bool]] = {}  # uri -> inspect_layout()

    # ---------- table resolution ----------
    def all_tables(self, refresh: bool = False) -> dict[str, TableConfig]:
        tables = {t.name: t for t in self.config.tables}
        configured_uris = {t.uri.rstrip("/") for t in self.config.tables}
        for t in self._discovered_tables(refresh):
            if t.uri.rstrip("/") in configured_uris:
                continue  # already configured explicitly (with its own name/description)
            name, i = t.name, 2
            while name in tables:  # explicit config wins; disambiguate collisions
                name = f"{t.name}_{i}"
                i += 1
            t.name = name
            tables[name] = t
        return tables

    def _discovered_tables(self, refresh: bool) -> list[TableConfig]:
        if not self.config.discover:
            return []
        with self._lock:
            fresh = self._discovered and (time.time() - self._discovered.at) < DISCOVERY_CACHE_SECONDS
            if fresh and not refresh:
                return [TableConfig(**vars(t)) for t in self._discovered.value]  # copies
        found: list[TableConfig] = []
        for d in self.config.discover:
            try:
                found.extend(discover_tables(d, self.config.default_storage_options))
            except Exception as e:  # keep serving configured tables if one prefix fails
                log.warning("Discovery failed for %s: %s", d.prefix, e)
        with self._lock:
            self._discovered = _Cached(found, time.time())
        return [TableConfig(**vars(t)) for t in found]

    def resolve(self, name: str) -> TableConfig:
        tables = self.all_tables()
        if name in tables:
            return self.with_format(tables[name])
        lowered = {k.lower(): v for k, v in tables.items()}
        if name.lower() in lowered:
            return self.with_format(lowered[name.lower()])
        known = ", ".join(sorted(tables)) or "(none configured)"
        raise UnknownTableError(f"Unknown table '{name}'. Known tables: {known}")

    def with_format(self, t: TableConfig) -> TableConfig:
        """Fill in `format` (when not configured) and, for plain files, `file_extension`: non-empty when the
        URI is a folder, which is then read as `<uri>/**/*<ext>`. Inspected once per URI, then cached."""
        if t.format in ("hudi", "delta") or t.file_extension or (t.format and has_glob(t.uri)):
            return t
        with self._lock:
            known = self._formats.get(t.uri)
        if not known:
            known = inspect_layout(t.uri, self.storage_options(t))
            with self._lock:
                self._formats[t.uri] = known
        fmt, ext, is_folder = known
        if not t.format and not fmt:
            raise ValueError(
                f"Cannot tell the format of table '{t.name}' ({t.uri}): it is not a Hudi or Delta table and has "
                "no .parquet/.csv/.json files. Set format: (and a glob uri if the files have other names)."
            )
        t.format = t.format or fmt
        if is_folder and not ext and t.format in DEFAULT_EXTENSIONS:
            ext = DEFAULT_EXTENSIONS[t.format]  # an empty folder configured with an explicit format
        t.file_extension = ext if is_folder else ""
        return t

    # ---------- options ----------
    def storage_options(self, t: TableConfig) -> dict[str, str]:
        """Defaults overlaid with the table's options. An empty value (e.g. from an unset `${VAR:-}`) means
        "not set", so a table can also blank out a default with `key: ""`."""
        merged = {**self.config.default_storage_options, **t.storage_options}
        return {k: v for k, v in merged.items() if v != ""}

    def hudi_options(self, t: TableConfig) -> dict[str, str]:
        return {**self.config.default_hudi_options, **t.hudi_options}

    # ---------- handles ----------
    def table(self, name: str, refresh: bool = False) -> HudiTable:
        t = self.resolve(name)
        if t.format != "hudi":
            raise ValueError(f"'{t.name}' is a {t.format} table, not a Hudi table")
        with self._lock:
            c = self._tables.get(t.name)
            if c and not refresh and (time.time() - c.at) < TABLE_CACHE_SECONDS:
                return c.value  # type: ignore[return-value]
        handle = (
            HudiTableBuilder.from_base_uri(t.uri)
            .with_storage_options(self.storage_options(t))
            .with_hudi_options(self.hudi_options(t))
            .build()
        )
        with self._lock:
            self._tables[t.name] = _Cached(handle, time.time())
        return handle

    # ---------- storage listings (describe / list) ----------
    def delta_version(self, t: TableConfig) -> int | None:
        """Latest committed version: the highest-numbered JSON file in `_delta_log/`."""
        filesystem, path = _pyarrow_fs(t.uri, self.storage_options(t))
        versions = [
            int(c.base_name[:-5])
            for c in _list(filesystem, f"{path}/_delta_log")
            if c.base_name.endswith(".json") and c.base_name[:-5].isdigit()
        ]
        return max(versions) if versions else None

    def data_files(self, t: TableConfig) -> list[tuple[str, int]]:
        """(path relative to the table root, size) of every data file of a parquet / csv / json table."""
        from pyarrow import fs as pafs

        if has_glob(t.uri):
            base = t.uri[: min(t.uri.index(c) for c in GLOB_CHARS if c in t.uri)].rsplit("/", 1)[0]
            pattern = t.uri[len(base) + 1:]
        else:
            base, pattern = t.uri, None
        filesystem, path = _pyarrow_fs(base, self.storage_options(t))
        info = filesystem.get_file_info(path)
        if info.type == pafs.FileType.File:
            return [(info.base_name, info.size or 0)]
        selector = pafs.FileSelector(path, recursive=True, allow_not_found=True)
        out = []
        for f in filesystem.get_file_info(selector):
            if f.type != pafs.FileType.File:
                continue
            rel = f.path[len(path):].lstrip("/")
            if any(part.startswith((".", "_")) for part in rel.split("/")):
                continue
            if pattern is not None and not fnmatch.fnmatch(rel, pattern):
                continue
            if pattern is None and not data_file_format(f.base_name):
                continue
            out.append((rel, f.size or 0))
        return out
