---
name: hudi-lake
description: Answer questions with data from the S3 data lake (Hudi, Delta Lake, Parquet, CSV and JSON tables, via the hudi MCP server), including Hudi history, time travel and what changed between loads.
---

# Querying the data lake

The `hudi` MCP server gives read-only SQL access to tables in S3: Apache Hudi, Delta Lake, Parquet, CSV and JSON.
Call its tools with `mcp_call`.

1. `list_tables` first if you don't already know the table names. Each table has a `format`.
2. `describe_table` before writing SQL: check column names/types and `partition_fields` (plus `record_key_fields`
   on Hudi tables).
3. `query` for analysis on the latest data. SQL dialect is DuckDB; one statement per call:
   - Always filter on partition columns when the question allows it (e.g. `WHERE dt >= '2026-09-01'`). This is
     what keeps queries fast: filters and selected columns are pushed into the table scans.
   - Tables of different formats can be joined in one query.
   - Aggregate in SQL instead of pulling raw rows; results are capped (`truncated: true` means incomplete).
   - Avoid `SELECT *` on large tables; name the columns you need.
   - Nested JSON fields are structs: `payload.user_id`.
   - DECIMAL values come back as exact strings.
4. History questions (Hudi tables only):
   - "How did X look on <date>?" → `hudi_query_as_of` with that date or a commit from `hudi_timeline`.
   - "What changed / was loaded since <time>?" → `hudi_query_incremental` with `start_timestamp`.
   - "When was the table last updated / how many rows did the last load write?" → `hudi_timeline`.
5. `_hoodie_commit_time` (commit that last wrote the row) and `_hoodie_record_key` exist on every Hudi table.
