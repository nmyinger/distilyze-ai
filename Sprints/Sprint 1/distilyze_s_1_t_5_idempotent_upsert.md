# Distilyze S1-T5 — Idempotent upsert by source keys

Prevent duplicates in the warehouse using natural keys `(source_system, txn_detail_integration_id)`. Load Parquet from MinIO and upsert into ClickHouse with `ReplacingMergeTree` and a dedup view.

---

## Tree

```
.
├─ orchestrator/
│  └─ distilyze_etl/
│     ├─ assets/
│     │  └─ upsert_gl.py
│     └─ __init__.py  # wire the new asset
├─ clickhouse/
│  └─ init/
│     └─ 002_raw_gl_line.sql  # optional ddl for reference
└─ Makefile
```

---

## ClickHouse DDL (reference)

`clickhouse/init/002_raw_gl_line.sql`

```sql
CREATE DATABASE IF NOT EXISTS distilyze;

CREATE TABLE IF NOT EXISTS distilyze.raw_gl_line
(
  source_system String,
  txn_detail_integration_id String,
  txn_integration_id Nullable(String),
  txn_id Nullable(String),
  txn_detail_id Nullable(String),

  posted_at Date,
  debit Decimal(18,2),
  credit Decimal(18,2),
  credit_debit_balance Nullable(Decimal(18,2)),

  account_integration_id String,
  account_id Nullable(String),
  unit_integration_id Nullable(String),
  unit_id Nullable(String),
  property_integration_id String,
  property_id Nullable(String),
  project_id Nullable(String),
  receivable_invoice_detail_id Nullable(String),
  party_id Nullable(String),
  party_type Nullable(String),

  reference Nullable(String),
  type Nullable(String),
  description Nullable(String),
  remarks Nullable(String),

  service_from Nullable(Date),
  service_to Nullable(Date),

  created_by Nullable(String),
  txn_created_at Nullable(DateTime),
  txn_updated_at Nullable(DateTime),
  invoice_updated_at Nullable(DateTime),
  account_updated_at Nullable(DateTime),

  amount_signed Decimal(18,2),
  load_date Date,
  ingested_at DateTime,
  row_fingerprint UInt64
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source_system, txn_detail_integration_id)
SETTINGS index_granularity = 8192;

-- Deduped view for consumers
CREATE VIEW IF NOT EXISTS distilyze.v_raw_gl_line AS
SELECT * FROM distilyze.raw_gl_line FINAL;
```

> The asset below will also create these objects if missing.

---

## assets/upsert_gl.py — idempotent loader

```python
from __future__ import annotations
from dagster import asset, AssetExecutionContext, Output
from datetime import datetime, timezone
from typing import List
import json, hashlib
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs
import clickhouse_connect
import pandas as pd

from distilyze_etl.contracts.gl import GLLine

DDL = """
CREATE TABLE IF NOT EXISTS distilyze.raw_gl_line
(
  source_system String,
  txn_detail_integration_id String,
  txn_integration_id Nullable(String),
  txn_id Nullable(String),
  txn_detail_id Nullable(String),
  posted_at Date,
  debit Decimal(18,2),
  credit Decimal(18,2),
  credit_debit_balance Nullable(Decimal(18,2)),
  account_integration_id String,
  account_id Nullable(String),
  unit_integration_id Nullable(String),
  unit_id Nullable(String),
  property_integration_id String,
  property_id Nullable(String),
  project_id Nullable(String),
  receivable_invoice_detail_id Nullable(String),
  party_id Nullable(String),
  party_type Nullable(String),
  reference Nullable(String),
  type Nullable(String),
  description Nullable(String),
  remarks Nullable(String),
  service_from Nullable(Date),
  service_to Nullable(Date),
  created_by Nullable(String),
  txn_created_at Nullable(DateTime),
  txn_updated_at Nullable(DateTime),
  invoice_updated_at Nullable(DateTime),
  account_updated_at Nullable(DateTime),
  amount_signed Decimal(18,2),
  load_date Date,
  ingested_at DateTime,
  row_fingerprint UInt64
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source_system, txn_detail_integration_id);

CREATE VIEW IF NOT EXISTS distilyze.v_raw_gl_line AS
SELECT * FROM distilyze.raw_gl_line FINAL;
"""

COLUMNS = [
  "source_system","txn_detail_integration_id","txn_integration_id","txn_id","txn_detail_id",
  "posted_at","debit","credit","credit_debit_balance",
  "account_integration_id","account_id","unit_integration_id","unit_id","property_integration_id",
  "property_id","project_id","receivable_invoice_detail_id","party_id","party_type",
  "reference","type","description","remarks",
  "service_from","service_to","created_by","txn_created_at","txn_updated_at",
  "invoice_updated_at","account_updated_at","amount_signed","load_date","ingested_at","row_fingerprint"
]


def _load_parquet(path: str, endpoint_url: str, access_key: str, secret_key: str, region: str) -> pd.DataFrame:
    fs = s3fs.S3FileSystem(
        client_kwargs={
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
        },
        use_listings_cache=False,
    )
    with fs.open(path, "rb") as f:
        table = pq.read_table(f)
    df = table.to_pandas()
    return df


def _fingerprint_row(rec: dict) -> int:
    s = json.dumps(rec, sort_keys=True, default=str)
    h = hashlib.blake2b(s.encode(), digest_size=8).digest()
    return int.from_bytes(h, "little", signed=False)


@asset(group_name="load")
def upsert_gl_to_clickhouse(
    context: AssetExecutionContext,
    extract_gl_lines: str,  # upstream path from S1-T4
    s3,
    clickhouse
) -> Output[int]:
    """Upsert normalized GL lines to ClickHouse with natural-key dedup."""
    if not extract_gl_lines:
        context.log.info("No upstream parquet path. Nothing to load.")
        return Output(0)

    # Read
    df = _load_parquet(
        extract_gl_lines,
        endpoint_url=s3.endpoint_url,
        access_key=s3.access_key,
        secret_key=s3.secret_key,
        region=s3.region,
    )

    # Normalize via contract again for safety and produce amount_signed
    rows = []
    for r in df.to_dict(orient="records"):
        obj = GLLine(**r)
        d = obj.model_dump()
        d["amount_signed"] = obj.amount_signed
        rows.append(d)
    df = pd.DataFrame(rows)

    # Append lineage fields
    now = datetime.now(timezone.utc)

    # Try to parse load_date from path like .../load_date=YYYY-MM-DD/...
    import re
    m = re.search(r"load_date=(\d{4}-\d{2}-\d{2})", extract_gl_lines)
    load_date = m.group(1) if m else now.date().isoformat()
    df["load_date"] = load_date
    df["ingested_at"] = now.replace(tzinfo=None)  # CH DateTime is naive UTC

    # Fingerprint for debug/lineage
    fp_cols = [c for c in df.columns if c not in {"ingested_at", "load_date"}]
    df["row_fingerprint"] = [ _fingerprint_row({k: r[k] for k in fp_cols if k in r}) for r in df.to_dict("records") ]

    # Ensure table exists and insert
    with clickhouse.client() as ch:
        ch.command(DDL)
        data = df[COLUMNS].to_dict(orient="records")
        ch.insert("distilyze.raw_gl_line", data, column_names=COLUMNS)

    context.log.info(f"Upserted {len(df)} rows into distilyze.raw_gl_line")
    return Output(len(df))
```

---

## __init__.py — wire the asset

```python
from dagster import Definitions
from .assets.bootstrap import init_storage
from .assets.contracts import gl_contract_schema, validate_and_stage_gl
from .assets.extract_gl import extract_gl_lines
from .assets.upsert_gl import upsert_gl_to_clickhouse
from .schedules import daily_heartbeat
from .resources.s3 import MinIOS3Resource
from .resources.clickhouse import ClickHouseResource
from .resources.http import HttpResource
import os

resources = {
    "s3": MinIOS3Resource(
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        access_key=os.environ.get("MINIO_ROOT_USER", "distilyze"),
        secret_key=os.environ.get("MINIO_ROOT_PASSWORD", "distilyze123"),
        region=os.environ.get("MINIO_REGION", "us-east-1"),
        verify=False,
    ),
    "clickhouse": ClickHouseResource(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "distilyze"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "distilyze123"),
        database=os.environ.get("CLICKHOUSE_DB", "distilyze"),
    ),
    "http": HttpResource(
        base_url=os.environ.get("GL_BASE_URL", "http://mock-gl:8080"),
        token=os.environ.get("GL_TOKEN"),
        timeout_s=float(os.environ.get("GL_TIMEOUT_S", "30")),
    ),
}

defs = Definitions(
    assets=[
        init_storage,
        gl_contract_schema,
        validate_and_stage_gl,
        extract_gl_lines,
        upsert_gl_to_clickhouse,
    ],
    schedules=[daily_heartbeat],
    resources=resources,
)
```

---

## Makefile — convenience

```make
.PHONY: gl-upsert gl-upsert-twice

gl-upsert:
	docker compose run --rm dagster-web bash -lc "python - <<'PY'\nfrom dagster import materialize\nfrom distilyze_etl.assets.extract_gl import extract_gl_lines\nfrom distilyze_etl.assets.upsert_gl import upsert_gl_to_clickhouse\nres = materialize([extract_gl_lines, upsert_gl_to_clickhouse])\nprint(res.success)\nPY"

gl-upsert-twice:
	$(MAKE) gl-upsert; $(MAKE) gl-upsert
```

---

## How idempotency works

- Natural key is `(source_system, txn_detail_integration_id)`.
- Table engine `ReplacingMergeTree(ingested_at)` keeps the latest version for a key.
- The consumer view `v_raw_gl_line` always returns deduped rows (`FINAL`).
- Running the loader multiple times with the same batch does not increase distinct keys.

---

## How to run

1) Rebuild and bring up Dagster.

```
docker compose build dagster-web dagster-daemon
```

2) Materialize the chain.

- In Dagster UI: materialize `extract_gl_lines` then `upsert_gl_to_clickhouse`.
- Or: `make gl-upsert`

3) Verify dedupe.

In ClickHouse client:

```
-- total inserted rows (raw with duplicates allowed)
SELECT count(*) FROM distilyze.raw_gl_line;

-- distinct keys
SELECT countDistinct(source_system, txn_detail_integration_id) FROM distilyze.raw_gl_line;

-- deduped view equals distinct keys
SELECT count(*) FROM distilyze.v_raw_gl_line;
```

Run `make gl-upsert-twice` and re-run the three queries. The last two counts stay the same.

---

## Acceptance criteria for T5

- Duplicate inserts for the same `(source_system, txn_detail_integration_id)` do not create extra rows in `v_raw_gl_line`.
- Running the loader twice produces identical `countDistinct` by key.
- Data is queryable via `SELECT * FROM distilyze.v_raw_gl_line` without duplicates.

