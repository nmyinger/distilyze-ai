# Distilyze S1-T4 — GL extract with paging via `next_page_url`

Implement a Dagster asset that pages a GL API until `next_page_url` is null, validates rows with the GL contract, and writes Parquet under `raw/gl_line/load_date=YYYY-MM-DD/` in MinIO.

---

## Tree

```
.
├─ orchestrator/
│  └─ distilyze_etl/
│     ├─ resources/
│     │  └─ http.py
│     ├─ extractors/
│     │  └─ gl_api.py
│     ├─ assets/
│     │  └─ extract_gl.py
│     └─ __init__.py   # add asset and resource
├─ orchestrator/pyproject.toml  # deps update
├─ docker-compose.yml           # env for GL API
└─ Makefile                     # helper targets
```

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
  "httpx>=0.27",
  "backoff>=2.2",
]
```

Rebuild the orchestrator image after editing.

```
docker compose build dagster-web dagster-daemon
```

---

## resources/http.py — generic HTTP client for GL API

```python
from dagster import ConfigurableResource
import httpx
from typing import Optional

class HttpResource(ConfigurableResource):
    base_url: str
    token: Optional[str] = None
    timeout_s: float = 30.0

    def client(self) -> httpx.Client:
        headers = {}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return httpx.Client(base_url=self.base_url, headers=headers, timeout=self.timeout_s)
```

---

## extractors/gl_api.py — pager that follows `next_page_url`

```python
from __future__ import annotations
from typing import Dict, Iterable, Iterator, Optional
import backoff
import httpx

EXPECTED_KEYS = ("data", "results", "items")

class GLPager:
    def __init__(self, client: httpx.Client):
        self.client = client

    @backoff.on_exception(backoff.expo, (httpx.HTTPError,), max_time=60)
    def _get(self, url: str, params: Optional[Dict] = None) -> Dict:
        r = self.client.get(url, params=params)
        r.raise_for_status()
        return r.json()

    def iter_lines(self, start_path: str, params: Optional[Dict] = None) -> Iterator[Dict]:
        url = start_path
        seen = 0
        while url:
            payload = self._get(url, params=params)
            params = None  # only for first request
            # find collection key
            rows = None
            for k in EXPECTED_KEYS:
                if k in payload and isinstance(payload[k], list):
                    rows = payload[k]
                    break
            if rows is None:
                raise ValueError("no rows found in response")
            for row in rows:
                yield row
                seen += 1
            url = payload.get("next_page_url") or payload.get("next") or None
        if seen == 0:
            # still OK. empty extraction
            return
```

---

## assets/extract_gl.py — Dagster asset that stages Parquet

```python
from __future__ import annotations
from dagster import asset, AssetExecutionContext, Output
from typing import Dict, Any, Iterable, List
from decimal import Decimal
from datetime import datetime, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs

from distilyze_etl.contracts.gl import GLLine
from distilyze_etl.resources.http import HttpResource
from distilyze_etl.extractors.gl_api import GLPager


def _normalize(records: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for r in records:
        obj = GLLine(**r)  # validates and normalizes
        d = obj.model_dump()
        d["amount_signed"] = obj.amount_signed
        rows.append(d)
    return pd.DataFrame(rows)


def _to_s3_parquet(df: pd.DataFrame, endpoint_url: str, access_key: str, secret_key: str, region: str, bucket: str, key_prefix: str) -> str:
    fs = s3fs.S3FileSystem(
        client_kwargs={
            "endpoint_url": endpoint_url,
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
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


@asset(group_name="extract")
def extract_gl_lines(context: AssetExecutionContext, http: HttpResource, s3) -> Output[str]:
    """
    Page the GL API using `next_page_url` until exhausted, validate rows with the GL contract,
    and stage a Parquet partition under raw/gl_line/.
    Environment variables control endpoint and filters.
    """
    import os

    # Endpoint and filters
    start_path = os.environ.get("GL_START_PATH", "/gl/lines")  # may be a path or absolute URL
    page_size = int(os.environ.get("GL_PAGE_SIZE", "500"))
    since = os.environ.get("GL_SINCE")  # ISO8601 optional
    until = os.environ.get("GL_UNTIL")  # ISO8601 optional

    params: Dict[str, Any] = {"page_size": page_size}
    if since:
        params["since"] = since
    if until:
        params["until"] = until

    # Iterate
    with http.client() as client:
        pager = GLPager(client)
        records = list(pager.iter_lines(start_path=start_path, params=params))

    if not records:
        context.log.info("No GL rows returned. Skipping write.")
        return Output("")

    df = _normalize(records)

    bucket = context.run_config.get("s3_raw_bucket", "distilyze-raw")
    path = _to_s3_parquet(
        df,
        endpoint_url=s3.endpoint_url,
        access_key=s3.access_key,
        secret_key=s3.secret_key,
        region=s3.region,
        bucket=bucket,
        key_prefix="gl_line",
    )
    context.log.info(f"Wrote {len(df)} rows to {path}")
    return Output(path)
```

---

## __init__.py — wire the new resource and asset

```python
from dagster import Definitions
from .assets.bootstrap import init_storage
from .assets.contracts import gl_contract_schema, validate_and_stage_gl
from .assets.extract_gl import extract_gl_lines
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
        base_url=os.environ.get("GL_BASE_URL", "http://localhost:8080"),
        token=os.environ.get("GL_TOKEN"),
        timeout_s=float(os.environ.get("GL_TIMEOUT_S", "30")),
    ),
}

defs = Definitions(
    assets=[init_storage, gl_contract_schema, validate_and_stage_gl, extract_gl_lines],
    schedules=[daily_heartbeat],
    resources=resources,
)
```

---

## docker-compose.yml — add GL env

```yaml
services:
  dagster-web:
    environment:
      GL_BASE_URL: ${GL_BASE_URL:-http://mock-gl:8080}
      GL_TOKEN: ${GL_TOKEN:-}
      GL_START_PATH: ${GL_START_PATH:-/gl/lines}
      GL_PAGE_SIZE: ${GL_PAGE_SIZE:-500}
      GL_SINCE: ${GL_SINCE:-}
      GL_UNTIL: ${GL_UNTIL:-}

  dagster-daemon:
    environment:
      GL_BASE_URL: ${GL_BASE_URL:-http://mock-gl:8080}
      GL_TOKEN: ${GL_TOKEN:-}
      GL_START_PATH: ${GL_START_PATH:-/gl/lines}
      GL_PAGE_SIZE: ${GL_PAGE_SIZE:-500}
      GL_SINCE: ${GL_SINCE:-}
      GL_UNTIL: ${GL_UNTIL:-}
```

Optional local mock later can run at `mock-gl:8080`.

---

## Makefile — helper targets

```make
.PHONY: extract-gl once-gl

extract-gl:
	docker compose run --rm dagster-web bash -lc "python - <<'PY'\nfrom dagster import materialize\nfrom distilyze_etl.assets.extract_gl import extract_gl_lines\nres = materialize([extract_gl_lines])\nprint(res.success)\nPY"

once-gl:
	docker compose run --rm dagster-web bash -lc "python - <<'PY'\nfrom distilyze_etl.resources.http import HttpResource\nfrom distilyze_etl.extractors.gl_api import GLPager\nimport os\nhr = HttpResource(base_url=os.environ.get('GL_BASE_URL'), token=os.environ.get('GL_TOKEN'))\nwith hr.client() as c:\n    pager = GLPager(c)\n    n=0\n    for _ in pager.iter_lines(os.environ.get('GL_START_PATH', '/gl/lines'), params={'page_size': int(os.environ.get('GL_PAGE_SIZE','500'))}):\n        n+=1\n    print('rows', n)\nPY"
```

---

## How to run

1) Rebuild orchestrator.

```
docker compose build dagster-web dagster-daemon
```

2) Set GL env in `.env`.

```
GL_BASE_URL=https://your-gl.example.com
GL_TOKEN=your_api_token
GL_START_PATH=/api/v1/gl/lines
GL_PAGE_SIZE=500
GL_SINCE=2025-07-01T00:00:00Z
```

3) One-off test.

```
make once-gl
```

4) Materialize the asset.

- Dagster UI → Assets → `extract_gl_lines` → Materialize
- Or run: `make extract-gl`

5) Verify output in MinIO.

`distilyze-raw/gl_line/load_date=YYYY-MM-DD/part-0000.parquet`

---

## Acceptance criteria for T4

- Extraction pages through the GL API until no `next_page_url`.
- Parquet partition written under `raw/gl_line/` with current `load_date`.
- Contract validation applied to each row via `GLLine` before write.
- Configurable via env without code changes.

