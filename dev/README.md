# Local test stack: nao + hudi-mcp + RustFS

`docker-compose.yml` at the repo root runs everything needed to try the MCP server from nao, with no AWS account:

| Service | What it does | URL |
|---|---|---|
| `rustfs` | S3-compatible object storage ([RustFS](https://rustfs.com)) | S3 API `http://localhost:9000`, console `http://localhost:9001/rustfs/console/` (`rustfsadmin` / `rustfsadmin`) |
| `seed` | One-shot: creates bucket `lake` and uploads sample tables (below) | |
| `hudi-mcp` | This repo's MCP server (`server` image), configured by `dev/hudi-mcp.yaml` | `http://localhost:8000/mcp` |
| `nao-project` | One-shot: copies `dev/nao-project/` and `nao/skills/` into the volume nao uses as its project | |
| `postgres` | nao's database (users, chats, settings survive restarts) | |
| `nao` | `getnao/nao:latest`, connected to `hudi-mcp` over streamable HTTP | `http://localhost:5005` |

## Run it

```bash
cp dev/.env.example .env          # add an LLM key: Anthropic, OpenAI or Amazon Bedrock
docker compose up -d --build
docker compose logs -f seed hudi-mcp nao
```

With Podman instead of Docker, `podman compose up -d --build` works the same way.

Then in nao (`http://localhost:5005`):

1. Sign up. The first account becomes the admin and gets the project.
2. **Settings → Agent → MCP servers**: the `hudi` server is listed. Click **Connect all MCP servers**; its seven tools
   appear.
3. Ask questions in a chat, for example:
   - "Which tables are in the lake and what format is each one?"
   - "What was the total order revenue in USD per region in September 2026?" (Parquet + Delta + CSV join)
   - "How many enterprise customers placed an order on 2026-09-15?"
   - "Which device generates the most checkout events?" (JSON with nested fields)
   - "How many trips were there before the deletes, and how many now?" (Hudi time travel)

## Sample data

Everything lives under `s3://lake/warehouse/`:

| Table | Format | Path |
|---|---|---|
| `txns` | Hudi copy-on-write, partitioned by region, 12 commits | `hudi/v9_txns_simple_meta` |
| `trips` | Hudi merge-on-read, 8 inserts then 3 deletes | `hudi/v6_trips_8i3d` |
| `v8_simplekeygen_nonhivestyle` | Hudi, picked up by discovery | `hudi/v8_simplekeygen_nonhivestyle` |
| `customers` | Delta Lake, 200 rows, partitioned by region, 2 versions | `delta/customers` |
| `orders` | Parquet, 5,000 rows, hive-partitioned by `dt` | `parquet/orders` |
| `fx_rates` | CSV, one file | `csv/fx_rates.csv` |
| `events` | Newline-delimited JSON, 2,000 rows, nested `context` | `json/events` |

The Hudi tables are the small sample tables from the hudi-rs repo, so `seed` needs internet access on its first run.
`orders.customer_id` and `events.customer_id` join to `customers`, and `orders.currency` joins to `fx_rates`.

## Using Amazon Bedrock as the LLM

Fill in `BEDROCK_AWS_REGION` and one of the three credential options in `.env`:

1. **Bedrock API key:** `AWS_BEARER_TOKEN_BEDROCK`.
2. **IAM access keys:** `BEDROCK_AWS_ACCESS_KEY_ID` / `BEDROCK_AWS_SECRET_ACCESS_KEY`, plus
   `BEDROCK_AWS_SESSION_TOKEN` for temporary credentials. To copy them from a profile, run
   `aws configure export-credentials --profile <profile> --format env`. Temporary credentials expire: update
   them and run `docker compose up -d nao`.
3. **A named profile, SSO included:** set `BEDROCK_AWS_PROFILE`, then uncomment the `~/.aws` volume of the `nao`
   service in `docker-compose.yml`. For SSO, run `aws sso login --profile <profile>` on the host first.

Compose hands these to nao alone, as `AWS_REGION`, `AWS_ACCESS_KEY_ID` and so on. The MCP server never sees them, so
Bedrock and S3 can use different accounts. After `docker compose up -d`, pick a Bedrock model in nao's model
selector (for example "Claude Sonnet 4.6 (Bedrock US)"). nao rewrites the model's cross-region prefix (`us.`,
`eu.`, `apac.`) to match the region, and the model must be enabled for your account in that region.

## Querying buckets in AWS S3

The sample tables live in RustFS. To add your own data:

1. Fill in `LAKE_AWS_REGION` and `LAKE_AWS_ACCESS_KEY_ID` / `LAKE_AWS_SECRET_ACCESS_KEY` in `.env`. Add
   `LAKE_AWS_SESSION_TOKEN` too if the credentials are temporary. The identity needs `s3:ListBucket` on the
   bucket and `s3:GetObject` on the table prefixes.
2. Uncomment and adapt the AWS examples in `dev/hudi-mcp.yaml`. There are examples for:
   - a table folder whose format is detected
   - a glob over many files
   - a single CSV file with reader options
   - a bucket in another region or account with its own keys
   - discovering a whole prefix
3. Run `docker compose up -d hudi-mcp`, then in nao reconnect the MCP server or start a new chat.

AWS tables use the config's `defaults`, which get the `LAKE_AWS_*` values. The RustFS tables carry their own
settings, which blank out the session token, so the two never mix. If an AWS table can't be opened (wrong
credentials, region or path), `list_tables` shows the error on that table and the other tables keep working.

## Poking at the pieces directly

```bash
# Call the MCP server without nao (MCP Inspector)
npx @modelcontextprotocol/inspector     # transport "Streamable HTTP", URL http://localhost:8000/mcp

# Browse the bucket
aws --endpoint-url http://localhost:9000 s3 ls --recursive s3://lake/warehouse/   # rustfsadmin / rustfsadmin
```

## Changing things

- **MCP server code:** `docker compose up -d --build hudi-mcp`.
- **MCP config** (`dev/hudi-mcp.yaml`): `docker compose restart hudi-mcp`.
- **nao project** (`dev/nao-project/`, `nao/skills/`): `docker compose up -d --force-recreate nao-project nao`.
- **Re-upload sample data:** `SEED_FORCE=1 docker compose up seed`.
- **Start from scratch** (drops the bucket, the nao database and the project volume): `docker compose down -v`.

The secrets in this stack (RustFS keys, Postgres password, `BETTER_AUTH_SECRET` default) are for local testing only.
