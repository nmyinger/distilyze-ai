# Distilyze S1-T3 — GL data contract and invariants

Define a strict GL line contract, encode invariants, add Great Expectations checks, and expose a Dagster-friendly validator that writes validated Parquet to MinIO `raw/gl_line/` partitions.

---

## Tree

```
.
├─ orchestrator/
│  └─ distilyze_etl/
│     ├─ contracts/
│     │  ├─ __init__.py
│     │  ├─ gl.py
│     │  └─ export_schema.py
│     ├─ validation/
│     │  └─ gl_expectations.py
│     ├─ assets/
│     │  ├─ bootstrap.py
│     │  └─ contracts.py
│     ├─ jobs/
│     │  └─ extract.py   # patch note below
│     ├─ resources/
│     │  ├─ s3.py
│     │  └─ clickhouse.py
│     ├─ schedules.py
│     └─ __init__.py
├─ orchestrator/pyproject.toml   # deps updated
├─ Makefile                      # targets updated
└─ samples/
   └─ gl_lines.jsonl
```

---

## Invariants (authoritative)

Natural key
- Unique on `[source_system, txn_detail_integration_id]`.

Amounts
- `debit` and `credit` are decimals in dollars, `>= 0.00` with 2 decimals.
- Exactly one of `debit` or `credit` is `> 0`. Never both `> 0`.
- `amount_signed = debit - credit`.
- If `credit_debit_balance` is provided, it must equal `amount_signed` within 0.01.

Dates
- `posted_at` is a date.
- `service_from` and `service_to` are both null or both set, and `service_from <= service_to`.

IDs and required fields
- Required: `source_system`, `txn_detail_integration_id`, `posted_at`, `account_integration_id`, `property_integration_id`, `debit`, `credit`.
- ID fields are trimmed, non-empty, and match `^[A-Za-z0-9._\-:/]+$`.

Timestamps order (soft check)
- If present: `txn_created_at <= txn_updated_at <= invoice_updated_at`.

---

## orchestrator/pyproject.toml — add deps

```toml
[project]
dependencies = [
  "dagster==1.8.9",
  "dagster-webserver==1.8.9",
  "dagster-postgres==0.24.9",
  "dagster-aws==0.24.9",
  "boto3>=1.34",
  "clickhouse-connect>=0.7.18",
  "psycopg2-binary>=2.9",
  "pydantic>=2.7",
  "great-expectations>=0.18.17",
  "pandas>=2.2",
  "pyarrow>=16.1",
  "s3fs>=2024.6.0",
]
```

Rebuild the image after editing.

```
docker compose build dagster-web dagster-daemon
```

---

## contracts/gl.py — Pydantic contract

```python
from __future__ import annotations
from decimal import Decimal, ROUND_HALF_UP
from datetime import date, datetime
import re
from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict

ID_RE = re.compile(r"^[A-Za-z0-9._\-:/]+$")

TWOPLACES = Decimal("0.01")

def q2(x: Decimal) -> Decimal:
    return x.quantize(TWOPLACES, rounding=ROUND_HALF_UP)

class GLLine(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    # Required identifiers
    source_system: str = Field(min_length=1)
    txn_detail_integration_id: str = Field(min_length=1)

    # Optional identifiers
    txn_integration_id: Optional[str] = None
    txn_id: Optional[str] = None
    txn_detail_id: Optional[str] = None

    # Accounting
    posted_at: date
    debit: Decimal = Field(default=Decimal("0.00"), ge=Decimal("0.00"))
    credit: Decimal = Field(default=Decimal("0.00"), ge=Decimal("0.00"))
    credit_debit_balance: Optional[Decimal] = None  # should equal debit - credit

    # Entity references
    account_integration_id: str = Field(min_length=1)
    account_id: Optional[str] = None
    unit_integration_id: Optional[str] = None
    unit_id: Optional[str] = None
    property_integration_id: str = Field(min_length=1)
    property_id: Optional[str] = None
    project_id: Optional[str] = None

    receivable_invoice_detail_id: Optional[str] = None
    party_id: Optional[str] = None
    party_type: Optional[str] = None  # vendor, tenant, employee, other

    # Free text
    reference: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None
    remarks: Optional[str] = None

    # Service window
    service_from: Optional[date] = None
    service_to: Optional[date] = None

    # Audit
    created_by: Optional[str] = None
    txn_created_at: Optional[datetime] = None
    txn_updated_at: Optional[datetime] = None
    invoice_updated_at: Optional[datetime] = None
    account_updated_at: Optional[datetime] = None

    # --- Validators ---
    @field_validator(
        "txn_detail_integration_id",
        "txn_integration_id",
        "txn_id",
        "txn_detail_id",
        "account_integration_id",
        "account_id",
        "unit_integration_id",
        "unit_id",
        "property_integration_id",
        "property_id",
        "project_id",
        "receivable_invoice_detail_id",
        "party_id",
        mode="before",
    )
    @classmethod
    def empty_to_none_and_pattern(cls, v: Optional[str]):
        if v is None:
            return v
        v = str(v).strip()
        if v == "":
            return None
        if not ID_RE.match(v):
            raise ValueError("invalid characters in id")
        return v

    @field_validator("debit", "credit", "credit_debit_balance", mode="before")
    @classmethod
    def coerce_decimal(cls, v):
        if v is None:
            return None
        if isinstance(v, Decimal):
            return q2(v)
        try:
            return q2(Decimal(str(v)))
        except Exception as e:
            raise ValueError("invalid decimal") from e

    @model_validator(mode="after")
    def check_amounts_and_dates(self):
        d = self.debit or Decimal("0.00")
        c = self.credit or Decimal("0.00")
        # Exactly one of debit or credit > 0
        if not ((d > 0) ^ (c > 0)):
            raise ValueError("exactly one of debit or credit must be > 0")
        # credit_debit_balance matches debit - credit if provided
        if self.credit_debit_balance is not None:
            diff = q2(d - c) - q2(self.credit_debit_balance)
            if abs(diff) > TWOPLACES:
                raise ValueError("credit_debit_balance must equal debit - credit within 0.01")
        # service window order
        if (self.service_from is None) ^ (self.service_to is None):
            raise ValueError("service_from and service_to must both be set or both null")
        if self.service_from and self.service_to and self.service_from > self.service_to:
            raise ValueError("service_from must be <= service_to")
        # timestamps order (soft)
        if self.txn_created_at and self.txn_updated_at:
            if self.txn_created_at > self.txn_updated_at:
                raise ValueError("txn_created_at must be <= txn_updated_at")
        if self.txn_updated_at and self.invoice_updated_at:
            if self.txn_updated_at > self.invoice_updated_at:
                raise ValueError("txn_updated_at must be <= invoice_updated_at")
        return self

    # Convenience
    @property
    def amount_signed(self) -> Decimal:
        return q2((self.debit or Decimal("0")) - (self.credit or Decimal("0")))
```

---

## contracts/export_schema.py — JSON Schema exporter

```python
from .gl import GLLine
import json, sys

if __name__ == "__main__":
    schema = GLLine.model_json_schema()
    out = json.dumps(schema, indent=2)
    sys.stdout.write(out)
```

Generate and save:

```
# writes schema next to the code on the host
mkdir -p orchestrator/distilyze_etl/contracts
docker compose run --rm dagster-web \
  python -m distilyze_etl.contracts.export_schema > orchestrator/distilyze_etl/contracts/gl_line.schema.json
```

---

## validation/gl_expectations.py — Great Expectations suite

```python
from __future__ import annotations
import pandas as pd
import great_expectations as gx
from datetime import date

REQUIRED = [
    "source_system",
    "txn_detail_integration_id",
    "posted_at",
    "account_integration_id",
    "property_integration_id",
    "debit",
    "credit",
]

ID_COLS = [
    "txn_detail_integration_id",
    "txn_integration_id",
    "txn_id",
    "txn_detail_id",
    "account_integration_id",
    "unit_integration_id",
    "property_integration_id",
    "project_id",
]

ID_REGEX = r"^[A-Za-z0-9._\-:/]+$"


def validate_with_ge(df: pd.DataFrame):
    v = gx.from_pandas(df)

    # Required non-null
    for col in REQUIRED:
        v.expect_column_values_to_not_be_null(col)

    # Types and ranges
    v.expect_column_values_to_be_between("debit", min_value=0)
    v.expect_column_values_to_be_between("credit", min_value=0)

    # Dates
    v.expect_column_values_to_not_be_null("posted_at")
    v.expect_column_values_to_be_between(
        "posted_at", min_value="2000-01-01", max_value="2100-01-01"
    )

    # Service window order when present
    v.expect_column_pair_values_A_to_be_less_than_or_equal_to_B(
        "service_from", "service_to", ignore_row_if="either_value_is_missing"
    )

    # ID shape
    for col in ID_COLS:
        if col in df.columns:
            v.expect_column_values_to_match_regex(col, ID_REGEX, mostly=0.999)

    # Natural key uniqueness
    v.expect_compound_columns_to_be_unique([
        "source_system",
        "txn_detail_integration_id",
    ])

    # Debit/Credit XOR as a boolean condition
    mask = (df["debit"] > 0) ^ (df["credit"] > 0)
    if not mask.all():
        bad = (~mask).sum()
        raise ValueError(f"{bad} rows violate XOR(debit, credit) rule")

    return v.validate()
```

---

## assets/contracts.py — Dagster validator and stager

```python
from __future__ import annotations
from dagster import asset, AssetExecutionContext, Output
from typing import Iterable, Dict, Any
from decimal import Decimal
from datetime import datetime, timezone
import pandas as pd

from distilyze_etl.contracts.gl import GLLine
from distilyze_etl.validation.gl_expectations import validate_with_ge

import io

@asset(group_name="contracts")
def gl_contract_schema(context: AssetExecutionContext) -> Output[str]:
    """Emit the JSON schema as text for discovery or registry publishing."""
    import json
    schema = GLLine.model_json_schema()
    s = json.dumps(schema, indent=2)
    context.log.info("GL schema built")
    return Output(s)


def _normalize(records: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for r in records:
        obj = GLLine(**r)
        d = obj.model_dump()
        d["amount_signed"] = obj.amount_signed
        rows.append(d)
    return pd.DataFrame(rows)


def _to_s3_parquet(df: pd.DataFrame, s3, bucket: str, key_prefix: str) -> str:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import s3fs

    fs = s3fs.S3FileSystem(
        client_kwargs={
            "endpoint_url": s3.endpoint_url,
            "aws_access_key_id": s3.access_key,
            "aws_secret_access_key": s3.secret_key,
            "region_name": s3.region,
        },
        use_listings_cache=False,
    )

    table = pa.Table.from_pandas(df)
    load_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{key_prefix}/load_date={load_date}/part-0000.parquet"
    path = f"s3://{bucket}/{key}"
    with fs.open(path, "wb") as f:
        pq.write_table(table, f)
    return path

@asset(group_name="contracts")
def validate_and_stage_gl(context: AssetExecutionContext, s3) -> Output[str]:
    """
    Validates a small sample batch and stages it to MinIO as Parquet.
    Replace the `records` list with actual extractor output in S1-T4.
    """
    # Demo payload (also provided under samples/gl_lines.jsonl)
    records = [
        {
            "source_system": "appfolio",
            "txn_detail_integration_id": "af:td:1001",
            "txn_integration_id": "af:t:5001",
            "posted_at": "2025-08-01",
            "debit": "125.00",
            "credit": "0",
            "account_integration_id": "6000:Repairs",
            "property_integration_id": "24MV",
            "description": "HVAC filter",
            "service_from": "2025-07-28",
            "service_to": "2025-07-28",
        },
        {
            "source_system": "appfolio",
            "txn_detail_integration_id": "af:td:1002",
            "txn_integration_id": "af:t:5001",
            "posted_at": "2025-08-01",
            "debit": "0",
            "credit": "125.00",
            "account_integration_id": "1100:Cash",
            "property_integration_id": "24MV",
            "description": "Counter entry",
        },
    ]

    df = _normalize(records)
    res = validate_with_ge(df)
    if not res["success"]:
        raise ValueError("Great Expectations suite failed")

    bucket = context.run_config.get("s3_raw_bucket", "distilyze-raw")
    path = _to_s3_parquet(df, s3, bucket=bucket, key_prefix="gl_line")
    context.log.info(f"Staged {len(df)} rows to {path}")
    return Output(path)
```

---

## __init__.py — include assets

```python
from dagster import Definitions
from .assets.bootstrap import init_storage
from .assets.contracts import gl_contract_schema, validate_and_stage_gl
from .schedules import daily_heartbeat
from .resources.s3 import MinIOS3Resource
from .resources.clickhouse import ClickHouseResource
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
}

defs = Definitions(
    assets=[init_storage, gl_contract_schema, validate_and_stage_gl],
    schedules=[daily_heartbeat],
    resources=resources,
)
```

---

## samples/gl_lines.jsonl — quick smoke

```json
{"source_system": "appfolio", "txn_detail_integration_id": "af:td:1001", "txn_integration_id": "af:t:5001", "posted_at": "2025-08-01", "debit": "125.00", "credit": "0", "account_integration_id": "6000:Repairs", "property_integration_id": "24MV", "description": "HVAC filter", "service_from": "2025-07-28", "service_to": "2025-07-28"}
{"source_system": "appfolio", "txn_detail_integration_id": "af:td:1002", "txn_integration_id": "af:t:5001", "posted_at": "2025-08-01", "debit": "0", "credit": "125.00", "account_integration_id": "1100:Cash", "property_integration_id": "24MV", "description": "Counter entry"}
```

---

## Makefile — add helpers

```make
.PHONY: contract-schema contract-validate

contract-schema:
	docker compose run --rm dagster-web \
		python -m distilyze_etl.contracts.export_schema > orchestrator/distilyze_etl/contracts/gl_line.schema.json
	@echo "Wrote orchestrator/distilyze_etl/contracts/gl_line.schema.json"

contract-validate:
	docker compose run --rm dagster-web python - <<'PY'
import json, pandas as pd
from distilyze_etl.contracts.gl import GLLine
from distilyze_etl.validation.gl_expectations import validate_with_ge

rows = [json.loads(l) for l in open('samples/gl_lines.jsonl')]
df = pd.DataFrame([GLLine(**r).model_dump() for r in rows])
res = validate_with_ge(df)
print(json.dumps(res, indent=2))
PY
```

---

## Patch note for S1-T2

Change the import inside `jobs/extract.py` to avoid a relative import error.

```diff
- from .resources.clickhouse import ClickHouseResource
+ from distilyze_etl.resources.clickhouse import ClickHouseResource
```

---

## How to run

1) Rebuild orchestrator to pick up deps.

```
docker compose build dagster-web dagster-daemon
```

2) Export the JSON Schema.

```
make contract-schema
```

3) Validate the sample and stage to MinIO.

- In Dagster UI, materialize `validate_and_stage_gl` from the Assets page. Or run:

```
docker compose run --rm dagster-web bash -lc "python -c 'from distilyze_etl.assets.contracts import validate_and_stage_gl'"
```

- Or use the Makefile smoke:

```
make contract-validate
```

4) Check MinIO for a new object.

`raw/gl_line/load_date=YYYY-MM-DD/part-0000.parquet` under the `distilyze-raw` bucket.

---

## Exit gate for T3

- JSON Schema present.
- GE suite passes on a sample batch.
- A Parquet partition lands in MinIO under `raw/gl_line/`.
- Invariants enforced for any extractor that uses `GLLine`.

