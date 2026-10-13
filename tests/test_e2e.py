"""End-to-end tests: spawn the MCP server over stdio and call every tool.

Runs against local sample Hudi tables plus generated Delta / Parquet / CSV / JSON
tables, and against the same tables uploaded to a local S3 (moto server), so the
S3 code path is exercised without AWS.

    ./scripts/fetch_sample_tables.sh ./sample_tables
    HUDI_SAMPLE_TABLES=./sample_tables pytest -v tests/
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import anyio
import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

SAMPLES = Path(os.environ.get("HUDI_SAMPLE_TABLES", "./sample_tables")).resolve()
TXNS = "v9_txns_simple_meta"      # COW, partitioned by region, 12 commits incl. replacecommits
TRIPS = "v6_trips_8i3d"           # MOR, partitioned by city, 8 inserts then 3 deletes

pytestmark = pytest.mark.skipif(not (SAMPLES / TXNS).exists(), reason="run scripts/fetch_sample_tables.sh first")


def call_all(config_path: Path, calls: list[tuple[str, dict]], env: dict | None = None) -> list:
    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "hudi_mcp.server", "--config", str(config_path), "--log-level", "WARNING"],
        env={**os.environ, **(env or {})},
    )

    async def go():
        out = []
        async with stdio_client(params) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                out.append(sorted(t.name for t in (await s.list_tools()).tools))
                for name, args in calls:
                    res = await s.call_tool(name, args, read_timeout_seconds=90)
                    if res.is_error:
                        out.append({"__error__": res.content[0].text})
                    else:
                        out.append(res.structured_content or json.loads(res.content[0].text))
        return out

    return anyio.run(go)


EXPECTED_TOOLS = [
    "describe_table", "hudi_query_as_of", "hudi_query_incremental", "hudi_timeline",
    "list_tables", "preview_table", "query",
]


def run_suite(config_path: Path, env: dict | None = None):
    res = call_all(
        config_path,
        [
            ("list_tables", {}),
            ("describe_table", {"table": "txns", "include_partitions": True}),
            ("preview_table", {"table": "txns", "limit": 3, "columns": ["txn_id", "amount", "region"]}),
            ("query", {"sql": "SELECT region, COUNT(*) AS n, ROUND(SUM(amount),2) AS amt FROM txns GROUP BY region ORDER BY region"}),
            ("query", {"sql": "SELECT city, COUNT(*) n FROM v6_trips_8i3d GROUP BY city ORDER BY city"}),
            ("hudi_timeline", {"table": "txns", "limit": 3}),
            ("hudi_query_as_of", {"table": "txns", "as_of_timestamp": "20260307162434124", "sql": "SELECT COUNT(*) AS n FROM txns"}),
            ("hudi_query_as_of", {"table": "v6_trips_8i3d", "as_of_timestamp": "20250618054711154", "sql": "SELECT COUNT(*) AS n FROM v6_trips_8i3d"}),
            ("hudi_query_incremental", {"table": "txns", "start_timestamp": "20260307162434124", "sql": "SELECT _hoodie_commit_time c, COUNT(*) n FROM txns GROUP BY 1 ORDER BY 1"}),
            ("query", {"sql": "DROP TABLE txns"}),
            ("query", {"sql": "SELECT * FROM nope"}),
            ("preview_table", {"table": "txns", "filters": [["region", "=", "eu"]], "columns": ["region"]}),
            ("query", {"sql": "SELECT txn_id, amount FROM txns t WHERE t.region = 'eu' AND amount > 0 ORDER BY txn_id"}),
            ("query", {"sql": "SELECT a.city, COUNT(*) n FROM v6_trips_8i3d a JOIN v6_trips_8i3d b ON a.uuid = b.uuid WHERE a.city = 'chennai' GROUP BY a.city"}),
        ],
        env,
    )
    (tools, listing, desc, preview, by_region, trips, tl, asof, asof_mor, incr, ddl, unknown, filtered,
     eu_rows, self_join) = res
    assert tools == EXPECTED_TOOLS
    names = {t["name"] for t in listing["tables"]}
    assert {"txns", "v6_trips_8i3d", "v8_simplekeygen_nonhivestyle"} <= names, listing
    assert all("error" not in t for t in listing["tables"]), listing

    assert desc["table_type"] == "COPY_ON_WRITE" and desc["partition_fields"] == ["region"]
    assert desc["record_key_fields"] == ["txn_id"] and desc["commits"]["count"] == 12
    assert sorted(desc["storage"]["partition_paths"]) == ["region=apac", "region=eu", "region=us"]

    assert preview["row_count"] == 3 and [c["name"] for c in preview["columns"]] == ["txn_id", "amount", "region"]
    assert by_region["rows"] == [
        {"region": "apac", "n": 4, "amt": "13808.25"},
        {"region": "eu", "n": 3, "amt": "1404.99"},
        {"region": "us", "n": 5, "amt": "7325.49"},
    ]
    assert sum(r["n"] for r in trips["rows"]) == 5          # MOR: 8 inserts - 3 deletes, merged
    assert tl["total_commits"] == 12 and len(tl["commits"]) == 3 and "operation" in tl["commits"][0]
    assert asof["rows"] == [{"n": 8}]                         # first commit only
    assert asof_mor["rows"] == [{"n": 8}]                     # MOR before the deletes
    assert sum(r["n"] for r in incr["rows"]) > 0 and len(incr["rows"]) > 1
    assert "__error__" in ddl and ("read-only" in ddl["__error__"].lower() or "not supported" in ddl["__error__"].lower()), ddl
    assert "__error__" in unknown and "Known tables" in unknown["__error__"]
    assert {r["region"] for r in filtered["rows"]} == {"eu"}
    assert eu_rows["row_count"] == 3 and [c["name"] for c in eu_rows["columns"]] == ["txn_id", "amount"]
    # region partition was pruned and only needed columns were read
    push = eu_rows["pushdown"]["txns"]
    assert push["rows_read"] == 3 and ["region", "=", "eu"] in push["filters_pushed"], push
    assert sorted(push["columns_read"]) == ["amount", "region", "txn_id"]
    assert self_join["pushdown"]["v6_trips_8i3d"]["filters_pushed"] == []  # table read twice: no pushdown
    assert self_join["rows"] and self_join["rows"][0]["city"] == "chennai"
    return res


@pytest.fixture(scope="session")
def formats_dir(tmp_path_factory) -> Path:
    """orders_delta (Delta, partitioned by region, 2 versions), orders_pq (hive-partitioned Parquet by dt),
    orders.csv (one file) and events/ (newline-delimited JSON)."""
    deltalake = pytest.importorskip("deltalake")
    import pyarrow as pa
    import pyarrow.csv as pacsv
    import pyarrow.parquet as pq

    root = tmp_path_factory.mktemp("formats")
    orders = pa.table({
        "order_id": [1, 2, 3, 4, 5, 6],
        "region": ["eu", "eu", "us", "us", "apac", "eu"],
        "amount": [10.5, 20.0, 7.25, 3.0, 99.9, 1.1],
        "dt": ["2026-09-01", "2026-09-01", "2026-09-02", "2026-09-02", "2026-09-03", "2026-09-03"],
    })
    deltalake.write_deltalake(str(root / "orders_delta"), orders, partition_by=["region"])
    deltalake.write_deltalake(
        str(root / "orders_delta"),
        pa.table({"order_id": [7], "region": ["us"], "amount": [5.0], "dt": ["2026-09-04"]}),
        mode="append",
    )
    pq.write_to_dataset(orders, str(root / "orders_pq"), partition_cols=["dt"])
    (root / "files").mkdir()
    pacsv.write_csv(orders, str(root / "files" / "orders.csv"))
    (root / "events").mkdir()
    for part, ids in enumerate([range(0, 3), range(3, 5)]):
        lines = [json.dumps({"event_id": i, "kind": "click" if i < 3 else "view", "payload": {"x": i}}) for i in ids]
        (root / "events" / f"part-{part}.json").write_text("\n".join(lines) + "\n")
    (root / "events" / "_SUCCESS").write_text("")
    return root


def run_formats_suite(config_path: Path, env: dict | None = None):
    res = call_all(
        config_path,
        [
            ("list_tables", {}),
            ("describe_table", {"table": "orders_delta", "include_partitions": True}),
            ("describe_table", {"table": "orders_pq", "include_partitions": True}),
            ("describe_table", {"table": "orders_csv"}),
            ("preview_table", {"table": "orders_csv", "limit": 5, "columns": ["order_id", "region"],
                               "filters": [["region", "=", "eu"], ["order_id", ">", "1"]]}),
            ("query", {"sql": "SELECT region, COUNT(*) n, ROUND(SUM(amount), 2) amt FROM orders_delta "
                              "GROUP BY region ORDER BY region"}),
            ("query", {"sql": "SELECT COUNT(*) n FROM orders_pq WHERE dt = '2026-09-02'"}),
            ("query", {"sql": "SELECT kind, COUNT(*) n, MAX(payload.x) mx FROM events GROUP BY kind ORDER BY kind"}),
            ("query", {"sql": "SELECT COUNT(*) n FROM txns t JOIN orders_pq o ON t.region = o.region"}),
            ("query", {"sql": "SELECT (SELECT COUNT(*) FROM read_text('/etc/passwd')) n FROM orders_pq"}),
            ("query", {"sql": "SELECT * FROM orders_pq, read_csv('s3://some-other-bucket/x.csv')"}),
            ("query", {"sql": "COPY (SELECT * FROM orders_pq) TO 'stolen.csv'"}),
            ("query", {"sql": "SELECT 1 FROM orders_pq; SELECT 2 FROM orders_pq"}),
            ("query", {"sql": "SELECT * FROM duckdb_secrets(), orders_pq"}),
            ("hudi_timeline", {"table": "orders_delta"}),
            ("query", {"sql": "SELECT COUNT(*) n FROM events_glob"}),
        ],
        env,
    )
    (_, listing, d_delta, d_pq, d_csv, preview, delta_q, pq_q, json_q, join_q,
     local_file, other_bucket, copy, multi, secrets, timeline, glob_q) = res
    formats = {t["name"]: t.get("format") for t in listing["tables"]}
    assert {"orders_delta": "delta", "orders_pq": "parquet", "events": "json", "orders_csv": "csv",
            "txns": "hudi"}.items() <= formats.items(), listing
    assert all("error" not in t for t in listing["tables"]), listing
    assert next(t for t in listing["tables"] if t["name"] == "orders_delta")["version"] == 1

    assert d_delta["partition_fields"] == ["region"] and d_delta["version"] == 1
    assert d_delta["storage"]["files"] == 4 and d_delta["storage"]["estimated_rows"] == 7
    assert sorted(d_delta["storage"]["partition_paths"]) == ["region=apac", "region=eu", "region=us"]
    assert {c["name"] for c in d_delta["columns"]} == {"order_id", "region", "amount", "dt"}
    assert d_pq["partition_fields"] == ["dt"] and d_pq["storage"]["rows"] == 6 and d_pq["storage"]["files"] == 3
    assert "dt" in {c["name"] for c in d_pq["columns"]}  # hive partition column is exposed
    assert d_csv["format"] == "csv" and d_csv["partition_fields"] == [] and d_csv["storage"]["files"] == 1

    assert preview["rows"] == [{"order_id": 2, "region": "eu"}, {"order_id": 6, "region": "eu"}]
    assert delta_q["rows"] == [
        {"region": "apac", "n": 1, "amt": 99.9},
        {"region": "eu", "n": 3, "amt": 31.6},
        {"region": "us", "n": 3, "amt": 15.25},
    ]
    assert delta_q["tables_used"] == {"orders_delta": "delta"} and delta_q["engine"] == "duckdb"
    assert pq_q["rows"] == [{"n": 2}]
    assert json_q["rows"] == [{"kind": "click", "n": 3, "mx": 2}, {"kind": "view", "n": 2, "mx": 4}]
    # Hudi (via hudi-rs) joined with Parquet (via DuckDB): apac 4x1 + eu 3x3 + us 5x2
    assert join_q["rows"] == [{"n": 23}] and join_q["tables_used"] == {"txns": "hudi", "orders_pq": "parquet"}
    assert "pushdown" in join_q and "txns" in join_q["pushdown"]

    for blocked in (local_file, other_bucket):
        assert "__error__" in blocked and "Permission Error" in blocked["__error__"], blocked
    assert "__error__" in copy and "read-only" in copy["__error__"], copy
    assert "__error__" in multi and "exactly one" in multi["__error__"], multi
    assert "__error__" in secrets and "not allowed" in secrets["__error__"], secrets
    assert "__error__" in timeline and "only works on Hudi" in timeline["__error__"], timeline
    assert glob_q["rows"] == [{"n": 5}]
    return res


def formats_config(formats_uri: str, samples_uri: str, header: str = "") -> str:
    return f"""{header}
tables:
  - name: txns
    uri: {samples_uri}/{TXNS}
  - name: orders_csv
    uri: {formats_uri}/files/orders.csv
  - name: events_glob
    uri: {formats_uri}/events/*.json
discover:
  - prefix: {formats_uri}
    max_depth: 1
    formats: [delta, parquet, json]
"""


def test_local_formats(tmp_path, formats_dir):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(formats_config(str(formats_dir), str(SAMPLES)))
    run_formats_suite(cfg)


def test_local(tmp_path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f"""
tables:
  - name: txns
    uri: {SAMPLES / TXNS}
    description: Card transactions
discover:
  - prefix: {SAMPLES}
    max_depth: 1
"""
    )
    res = run_suite(cfg)
    assert res[4]["engine"] == "duckdb"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def s3_endpoint(formats_dir):
    pytest.importorskip("moto")
    import boto3

    port = _free_port()
    proc = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-p", str(port)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    endpoint = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.2)
    s3 = boto3.client("s3", endpoint_url=endpoint, aws_access_key_id="test",
                      aws_secret_access_key="test", region_name="us-east-1")
    s3.create_bucket(Bucket="lake")
    for table in SAMPLES.iterdir():
        for f in table.rglob("*"):
            if f.is_file():
                s3.upload_file(str(f), "lake", f"warehouse/{table.name}/{f.relative_to(table)}")
    for f in formats_dir.rglob("*"):
        if f.is_file():
            s3.upload_file(str(f), "lake", f"formats/{f.relative_to(formats_dir)}")
    yield endpoint
    proc.terminate()


def test_s3(tmp_path, s3_endpoint):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f"""
defaults:
  storage_options:
    aws_endpoint: {s3_endpoint}
    aws_allow_http: true
    aws_region: us-east-1
    aws_access_key_id: ${{TEST_KEY}}
    aws_secret_access_key: ${{TEST_SECRET}}
tables:
  - name: txns
    uri: s3://lake/warehouse/{TXNS}
discover:
  - prefix: s3://lake/warehouse
    max_depth: 2
"""
    )
    res = run_suite(cfg, env={"TEST_KEY": "test", "TEST_SECRET": "test"})
    assert res[4]["engine"] == "duckdb"


def test_s3_formats(tmp_path, s3_endpoint):
    cfg = tmp_path / "config.yaml"
    header = f"""
defaults:
  storage_options:
    aws_endpoint: {s3_endpoint}
    aws_allow_http: true
    aws_region: us-east-1
    aws_access_key_id: test
    aws_secret_access_key: test
"""
    cfg.write_text(formats_config("s3://lake/formats", "s3://lake/warehouse", header))
    run_formats_suite(cfg)
