"""Fill the local RustFS bucket with one table of every format the MCP server reads.

    s3://lake/warehouse/hudi/<sample>/      Hudi sample tables from the hudi-rs repo (COW + MOR)
    s3://lake/warehouse/delta/customers/    Delta Lake, partitioned by region
    s3://lake/warehouse/parquet/orders/     Parquet, hive-partitioned by dt
    s3://lake/warehouse/csv/fx_rates.csv    a single CSV file
    s3://lake/warehouse/json/events/        newline-delimited JSON with nested fields

The generated tables join on customer_id (orders, events -> customers) and currency (orders -> fx_rates).
Runs once: a `_seeded` marker object makes later runs a no-op (set SEED_FORCE=1 to re-upload).
"""

from __future__ import annotations

import datetime as dt
import io
import json
import os
import random
import tempfile
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from deltalake import write_deltalake

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://rustfs:9000")
BUCKET = os.environ.get("S3_BUCKET", "lake")
PREFIX = "warehouse"
HUDI_SAMPLES = {
    "v9_txns_simple_meta": "sample_table/cow/v9_txns_simple_meta",
    "v8_simplekeygen_nonhivestyle": "sample_table/cow/v8_simplekeygen_nonhivestyle",
    "v6_trips_8i3d": "quickstart_trips_table/mor/avro/v6_trips_8i3d",
}
HUDI_BASE = "https://raw.githubusercontent.com/apache/hudi-rs/main/crates/test/data"

REGIONS = ["apac", "eu", "us"]
CURRENCIES = {"apac": "SGD", "eu": "EUR", "us": "USD"}

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("AWS_REGION", "us-east-1"),
    config=Config(s3={"addressing_style": "path"}),
)


def wait_for_bucket() -> None:
    for attempt in range(60):
        try:
            s3.head_bucket(Bucket=BUCKET)
            return
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
                s3.create_bucket(Bucket=BUCKET)
                print(f"Created bucket {BUCKET}")
                return
            print(f"Waiting for RustFS ({e.response['Error']['Code']})...")
        except EndpointConnectionError:
            print("Waiting for RustFS...")
        time.sleep(2)
    raise SystemExit(f"RustFS at {ENDPOINT} did not come up")


def upload_dir(local: Path, key_prefix: str) -> int:
    n = 0
    for f in sorted(local.rglob("*")):
        if f.is_file():
            s3.upload_file(str(f), BUCKET, f"{key_prefix}/{f.relative_to(local).as_posix()}")
            n += 1
    return n


def hudi_samples(work: Path) -> None:
    for name, path in HUDI_SAMPLES.items():
        with urllib.request.urlopen(f"{HUDI_BASE}/{path}.zip", timeout=120) as resp:
            zipfile.ZipFile(io.BytesIO(resp.read())).extractall(work / "hudi")
        n = upload_dir(work / "hudi" / name, f"{PREFIX}/hudi/{name}")
        print(f"hudi/{name}: {n} files")


def generated_tables(work: Path) -> None:
    rnd = random.Random(42)
    start = dt.date(2026, 9, 1)

    customers = pa.table({
        "customer_id": list(range(1, 201)),
        "name": [f"Customer {i:03d}" for i in range(1, 201)],
        "region": [REGIONS[i % 3] for i in range(1, 201)],
        "segment": [rnd.choice(["consumer", "smb", "enterprise"]) for _ in range(200)],
        "signup_date": [start - dt.timedelta(days=rnd.randint(0, 720)) for _ in range(200)],
    })
    write_deltalake(str(work / "customers"), customers, partition_by=["region"])
    # A second version, so the table has history: ten customers move to enterprise.
    upgraded = customers.slice(0, 10).set_column(3, "segment", pa.array(["enterprise"] * 10))
    write_deltalake(
        str(work / "customers"), upgraded, mode="overwrite", predicate="customer_id <= 10",
        partition_by=["region"],
    )
    print(f"delta/customers: {upload_dir(work / 'customers', f'{PREFIX}/delta/customers')} files")

    rows = []
    for order_id in range(1, 5001):
        customer_id = rnd.randint(1, 200)
        region = REGIONS[customer_id % 3]
        day = start + dt.timedelta(days=rnd.randint(0, 29))
        rows.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "amount": round(rnd.uniform(5, 500), 2),
            "currency": CURRENCIES[region],
            "status": rnd.choices(["completed", "refunded", "cancelled"], weights=[90, 6, 4])[0],
            "dt": day.isoformat(),
        })
    orders = pa.Table.from_pylist(rows)
    pq.write_to_dataset(orders, str(work / "orders"), partition_cols=["dt"], basename_template="part-{i}.parquet")
    print(f"parquet/orders: {upload_dir(work / 'orders', f'{PREFIX}/parquet/orders')} files")

    fx = pa.table({
        "currency": ["USD", "EUR", "SGD"],
        "rate_to_usd": [1.0, 1.09, 0.74],
        "as_of": ["2026-09-30"] * 3,
    })
    pacsv.write_csv(fx, str(work / "fx_rates.csv"))
    s3.upload_file(str(work / "fx_rates.csv"), BUCKET, f"{PREFIX}/csv/fx_rates.csv")
    print("csv/fx_rates.csv: 1 file")

    (work / "events").mkdir()
    for part in range(4):
        with open(work / "events" / f"part-{part}.json", "w") as f:
            for i in range(500):
                ts = dt.datetime(2026, 9, 1) + dt.timedelta(minutes=rnd.randint(0, 30 * 24 * 60))
                f.write(json.dumps({
                    "event_id": part * 500 + i,
                    "customer_id": rnd.randint(1, 200),
                    "event_type": rnd.choice(["page_view", "add_to_cart", "checkout"]),
                    "ts": ts.isoformat(),
                    "context": {"device": rnd.choice(["ios", "android", "web"]), "page": f"/p/{rnd.randint(1, 50)}"},
                }) + "\n")
    print(f"json/events: {upload_dir(work / 'events', f'{PREFIX}/json/events')} files")


def main() -> None:
    wait_for_bucket()
    marker = f"{PREFIX}/_seeded"
    try:
        s3.head_object(Bucket=BUCKET, Key=marker)
        if os.environ.get("SEED_FORCE") != "1":
            print(f"s3://{BUCKET}/{PREFIX} is already seeded (set SEED_FORCE=1 to re-upload)")
            return
    except ClientError:
        pass
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        hudi_samples(work)
        generated_tables(work)
    s3.put_object(Bucket=BUCKET, Key=marker, Body=b"")
    print(f"Seeded s3://{BUCKET}/{PREFIX}")


if __name__ == "__main__":
    main()
