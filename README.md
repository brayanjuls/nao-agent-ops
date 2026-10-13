# hudi-mcp

An MCP server that lets AI agents ([nao](https://getnao.io), Claude, Cursor, anything that speaks MCP) query
data-lake tables in **S3** with SQL: **Apache Hudi**, **Delta Lake**, **Parquet**, **CSV** and **JSON**. All SQL
runs in [DuckDB](https://duckdb.org/). Hudi tables are read natively with [hudi-rs](https://github.com/apache/hudi-rs)
(no Spark, no JVM); the other formats are read by DuckDB itself. It is read-only.

- One SQL dialect for every format, with joins across formats (a Hudi table joined to a CSV file, for example)
- Hudi copy-on-write and merge-on-read tables (MOR log files are merged, so upserts and deletes are reflected)
- Hudi time travel, incremental (CDC-style) reads and commit timeline
- Delta Lake tables (with partition and file-statistics pruning), hive-partitioned Parquet / CSV / JSON folders,
  single files and globs
- Explicit tables and/or **auto-discovery** of every table under an S3 prefix
- Column and filter pushdown for every format, plus row caps and a query timeout so an agent can't pull a
  whole lake
- A locked-down DuckDB: agent SQL can read only the configured tables (see [Security](#security))

## Tools

| Tool | What the agent uses it for |
|---|---|
| `list_tables` | Tables available, with format (Hudi: type, partition columns, latest commit; Delta: version) |
| `describe_table` | Columns and types, partition fields, size and row counts (Hudi: record key, commit range) |
| `preview_table` | Sample rows (stops reading once it has enough) |
| `query` | SQL over the latest data; joins across tables and formats |
| `hudi_timeline` | Hudi commits, newest first, with operation and inserted/updated/deleted counts |
| `hudi_query_as_of` | SQL over a Hudi table as of a past commit or time |
| `hudi_query_incremental` | SQL over records written by Hudi commits in `(start, end]` |

SQL is DuckDB's dialect. Exactly one `SELECT` / `WITH` / `EXPLAIN` statement per call is accepted. DECIMAL values
come back as exact strings.

## Quick start

```bash
pip install .            # Python 3.10+; pulls hudi 0.5, duckdb 1.5, mcp 2.x, sqlglot

# Quickest: list tables in an env var (credentials come from the normal AWS env vars / IAM role).
# The format of each table is detected from its layout.
export AWS_REGION=us-east-1
export HUDI_MCP_TABLES="trips=s3://my-lake/warehouse/trips,fx=s3://my-lake/ref/fx_rates.csv"
hudi-mcp                 # stdio

# Or a config file (recommended): see config.example.yaml
HUDI_MCP_CONFIG=./config.yaml hudi-mcp
hudi-mcp --config ./config.yaml --transport streamable-http --host 0.0.0.0 --port 8000   # HTTP at /mcp
```

**To try it end to end without AWS**, `docker compose up` starts nao, this server and RustFS (S3-compatible
storage) seeded with a table of every format. See [dev/README.md](dev/README.md).

DuckDB needs its `httpfs`, `aws` and `delta` extensions. The Docker images bake them in. Elsewhere they are
downloaded from DuckDB's extension repository on first use, into `~/.duckdb/extensions` or `duckdb.extension_directory`.

### Configuration

`config.example.yaml` documents every option. The essentials:

```yaml
defaults:
  storage_options:            # object_store-style keys, used for every format
    aws_region: us-east-1
tables:
  - name: trips               # the SQL name the agent uses
    uri: s3://my-lake/warehouse/trips
    description: One row per ride, partitioned by city.   # shown to the agent
  - name: customers
    uri: s3://my-lake/delta/customers
    format: delta             # optional: hudi | delta | parquet | csv | json (detected when left out)
  - name: fx_rates
    uri: s3://my-lake/reference/fx_rates.csv
    read_options: {delim: ";"}   # passed to DuckDB's read_csv / read_json / read_parquet
discover:
  - prefix: s3://my-lake/warehouse
    max_depth: 2
```

- **Table URIs** can be a table folder, a single file or a glob (`s3://lake/raw/2026-*/*.json.gz`). Folders
  of plain files are read recursively, and `key=value` partition folders become columns.
- **Format detection** (when `format:` is left out): `.hoodie/hoodie.properties` means Hudi, `_delta_log/` means
  Delta, otherwise the data files' extension decides: `.parquet`, `.csv` / `.tsv`, `.json` / `.jsonl` /
  `.ndjson`, optionally `.gz` / `.zst` compressed.
- `${VAR}` / `${VAR:-default}` placeholders are filled from the environment.
- **Credentials:** leave them out to use the AWS default chain: `AWS_*` environment variables, an EC2 instance
  role, an ECS task role or EKS IRSA. DuckDB (Delta / Parquet / CSV / JSON) also reads `~/.aws` profiles and SSO.
  hudi-rs does **not**, so for Hudi tables with SSO/profile setups locally run
  `eval "$(aws configure export-credentials --format env)"` first.
- **S3-compatible stores** (MinIO, Ceph, R2): set `aws_endpoint`, plus `aws_allow_http: true` for plain HTTP.
- **Merging storage options:** a table's `storage_options` are laid over `defaults`. An empty value (for example
  from an unset `${VAR:-}`) counts as unset, so `aws_session_token: ""` on a table removes a default. When a table
  sets its own `aws_access_key_id`, no credential (key, secret, session token, profile) is taken from the
  `AWS_*` environment for it.
- **Discovery** makes a table of every folder that holds one, named after the folder: Hudi tables, Delta tables,
  and folders of `.parquet` / `.csv` / `.json` files (directly or under `key=value` partition folders, with no
  other sub-folders). It never descends into a table. Limit it with `formats: [hudi, delta]`. Explicitly
  configured tables win over discovered ones with the same URI. Results are cached for 5 minutes
  (`HUDI_MCP_DISCOVERY_CACHE_SECONDS`); `list_tables(refresh=true)` rescans.
- IAM needs `s3:ListBucket` on the bucket and `s3:GetObject` on the table prefixes. Nothing is ever written.

## Adding it to nao

nao loads MCP servers from `agent/mcps/mcp.json` in your context repo, and the agent calls their tools through
`mcp_call`.

### Self-hosted nao (stdio)

1. Make `hudi-mcp` available inside the nao container. `Dockerfile` has a `nao` target that layers it onto
   `getnao/nao:latest` in its own virtualenv:
   ```bash
   docker build --target nao -t my-nao .
   ```
   Use `my-nao` wherever you ran `getnao/nao`.
2. Put your config somewhere in the context repo (e.g. `hudi/config.yaml`) and copy `nao/mcp.json` to
   `agent/mcps/mcp.json`, pointing `--config` at it.
3. Set `AWS_REGION` and credentials (or rely on the instance/task role). nao resolves `${VAR}` placeholders in
   `mcp.json` and lists them under **Settings → Project Settings & Budget → Environment Variables**, so secrets
   never go in git.
4. In **Settings → Agent → MCP servers**, click **Connect all MCP servers**. All seven tools are read-only.
5. Optional: copy `nao/skills/hudi-lake.md` to `agent/skills/` and add a line to `RULES.md` saying which
   questions should use the Hudi lake. This steers the agent toward partition filters and the Hudi time-travel tools.

### nao Cloud (HTTP)

nao's cloud mode only starts HTTP MCP servers. Run the `server` image somewhere nao can reach
(ECS/EKS next to your lake works well, using a task role for S3):

```bash
docker build --target server -t hudi-mcp .
docker run -p 8000:8000 -e HUDI_MCP_CONFIG=/config/config.yaml -v $PWD/config.yaml:/config/config.yaml hudi-mcp
```

then use `nao/mcp.remote.json`. The server has no built-in authentication, so keep it on a private network
or behind an authenticating proxy.

## How queries run

`query` runs every statement in DuckDB. Each table the SQL references is bound in one of two ways:

- **Delta, Parquet, CSV, JSON:** a view over DuckDB's own reader (`delta_scan`, `read_parquet`, `read_csv`,
  `read_json`). DuckDB pushes the selected columns and the filters into the scan, pruning partitions, Delta files
  (from their statistics) and Parquet row groups.
- **Hudi:** the SQL is analysed with sqlglot to work out which columns each Hudi table needs and which
  `column <op> literal` conditions can prune partitions and files. hudi-rs reads only that into Arrow, and DuckDB
  runs the full SQL on the result, so every filter is still applied exactly. The pushdown is conservative: nothing
  from `OR`, `ON` clauses or tables referenced more than once. Each result includes a `pushdown` block (columns
  read, filters pushed, rows read) per Hudi table so you can see what happened.

Because Hudi reads are materialized, each Hudi table read is capped at `max_scan_rows` (5,000,000 by default). When
a query would exceed it, the agent gets an error asking for partition filters instead of the server running out of
memory. Time-travel and incremental reads use the same cap. The other formats are streamed by DuckDB, which spills
to disk when a query needs more than `duckdb.memory_limit`. Every query is cancelled after
`limits.query_timeout_seconds` (300 by default).

## Security

DuckDB can read files and URLs from SQL, so the server locks it down before any agent SQL runs:

- Only the paths of the configured Delta / Parquet / CSV / JSON tables are readable (`allowed_directories` /
  `allowed_paths` with `enable_external_access = false`). `read_text('/etc/passwd')`, `read_csv('s3://another-bucket/…')`,
  `ATTACH` and `INSTALL` fail with a permission error.
- `lock_configuration = true`, so SQL cannot `SET` any of that back. Extension auto-install and auto-load are off.
- Each call must be a single `SELECT` / `WITH` / `EXPLAIN` statement, as classified by DuckDB's own parser.
  `COPY`, `CREATE`, `INSERT`, `PRAGMA` and the like are rejected. So are `duckdb_secrets()` / `which_secret()`
  (credential introspection) and `query()` / `query_table()` (SQL built from strings).

S3 credentials are held in DuckDB secrets: `PROVIDER credential_chain` when none are configured, or one secret
per table with its own `storage_options`, scoped to that table's prefix. The server has no built-in
authentication, so keep its HTTP endpoint on a private network or behind an authenticating proxy.

## Development

```bash
pip install -e ".[test]"
./scripts/fetch_sample_tables.sh ./sample_tables     # small COW + MOR tables from the hudi-rs repo
HUDI_SAMPLE_TABLES=./sample_tables pytest -v tests/
```

The tests start the server over stdio and call every tool against local tables and against the same tables
uploaded to a local S3 server (moto). Besides the Hudi sample tables, they generate a partitioned Delta table, a
hive-partitioned Parquet folder, a CSV file and a JSON folder (needs `deltalake`, in the `test` extra). They check
results such as MOR deletes being applied, time travel before the deletes, Hudi partition pruning, a Hudi ×
Parquet join, format detection and discovery, and that writes, multiple statements and reads outside the
configured tables are rejected.

## Limitations

- Read-only by design.
- Requires `hudi>=0.5,<0.6`: 0.5 replaced the earlier `read_snapshot()` API with `HudiReadOptions` + `read()`.
- Delta, Parquet, CSV and JSON tables can live on S3 (and S3-compatible stores) or local disk. Hudi tables can
  also use any store hudi-rs supports.
- Time travel and change queries are Hudi-only for now. DuckDB's `delta_scan` can read older Delta versions,
  but no tool exposes that yet.
- Hudi tables are still materialized in memory per query (see above). Streaming them into DuckDB is a
  possible next step.
- Incremental reads aren't streamed (hudi-rs doesn't support it yet), so the commit range has to fit in
  `max_scan_rows`.
- Hudi filters are typed against the column. If hudi-rs can't parse a pushed literal for a column type, that
  table is re-read without filters, which is correct but slower.
