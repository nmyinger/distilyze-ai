# Distilyze S1-T2 — Dagster init and scheduler

Scaffold a Dagster project, add a daily scheduler, and run it in Docker next to Postgres, ClickHouse, and MinIO.

---

## Tree

```
.
├─ orchestrator/
│  ├─ Dockerfile
│  ├─ pyproject.toml
│  ├─ workspace.yaml
│  ├─ dagster.yaml
│  └─ distilyze_etl/
│     ├─ __init__.py
│     ├─ assets/
│     │  └─ bootstrap.py
│     ├─ jobs/
│     │  └─ extract.py
│     ├─ resources/
│     │  ├─ s3.py
│     │  └─ clickhouse.py
│     └─ schedules.py
├─ docker-compose.yml  # add services at bottom
└─ Makefile            # add targets at bottom
```

---

## orchestrator/Dockerfile

```dockerfile
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster

WORKDIR /opt/code

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && rm -rf /var/lib/apt/lists/*

# Python deps
COPY orchestrator/pyproject.toml ./pyproject.toml
RUN pip install --upgrade pip && pip install -e .

# Instance and workspace
COPY orchestrator/dagster.yaml /opt/dagster/dagster.yaml
COPY orchestrator/workspace.yaml /opt/code/workspace.yaml

# Project code
COPY orchestrator/distilyze_etl /opt/code/distilyze_etl

# Dagster needs a writable home
RUN mkdir -p /opt/dagster && chmod -R 777 /opt/dagster

EXPOSE 3000
```

---

## orchestrator/pyproject.toml

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "distilyze-etl"
version = "0.1.0"
description = "Distilyze ETL code location"
requires-python = ">=3.11"
dependencies = [
  "dagster==1.8.9",
  "dagster-webserver==1.8.9",
  "dagster-postgres==0.24.9",
  "dagster-aws==0.24.9",
  "boto3>=1.34",
  "clickhouse-connect>=0.7.18",
  "psycopg2-binary>=2.9",
]

[tool.setuptools]
package-dir = {"" = "."}
packages = ["distilyze_etl", "distilyze_etl.assets", "distilyze_etl.jobs", "distilyze_etl.resources"]
```

---

## orchestrator/workspace.yaml

```yaml
load_from:
  - python_module: distilyze_etl
```

---

## orchestrator/dagster.yaml

```yaml
instance_class: dagster._core.instance.DagsterInstance

# Use Postgres for runs, events, and schedules
run_storage:
  module: dagster_postgres.run_storage
  class: PostgresRunStorage
  config:
    postgres_url: ${DAGSTER_PGURL}

event_log_storage:
  module: dagster_postgres.event_log
  class: PostgresEventLogStorage
  config:
    postgres_url: ${DAGSTER_PGURL}

schedule_storage:
  module: dagster_postgres.schedule_storage
  class: PostgresScheduleStorage
  config:
    postgres_url: ${DAGSTER_PGURL}

telemetry:
  enabled: false

local_artifact_storage:
  module: dagster._core.storage.root
  class: LocalArtifactStorage
  config:
    base_dir: /opt/dagster/storage

# Default logs location in container
compute_logs:
  module: dagster._core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: /opt/dagster/logs
```

---

## orchestrator/distilyze_etl/resources/s3.py

```python
from dagster import ConfigurableResource
import boto3

class MinIOS3Resource(ConfigurableResource):
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"
    verify: bool = False  # allow self-signed in local

    def client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            verify=self.verify,
        )
```

---

## orchestrator/distilyze_etl/resources/clickhouse.py

```python
from dagster import ConfigurableResource
import clickhouse_connect

class ClickHouseResource(ConfigurableResource):
    host: str
    port: int = 8123
    username: str
    password: str
    database: str

    def client(self):
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.username,
            password=self.password,
            database=self.database,
        )
```

---

## orchestrator/distilyze_etl/assets/bootstrap.py

```python
from dagster import asset, AssetExecutionContext

@asset(group_name="bootstrap")
def init_storage(context: AssetExecutionContext):
    context.log.info("Init storage placeholder. Real buckets already created by mc-init.")
    return {"ok": True}
```

---

## orchestrator/distilyze_etl/jobs/extract.py

```python
from dagster import job, op
from dagster import Definitions
from dagster import get_dagster_logger
from datetime import datetime
from .resources.clickhouse import ClickHouseResource

@op
def heartbeat_op(ch: ClickHouseResource):
    logger = get_dagster_logger()
    with ch.client() as client:
        client.command("""
            CREATE TABLE IF NOT EXISTS _heartbeat (
              ts DateTime DEFAULT now(),
              note String
            ) ENGINE = MergeTree ORDER BY ts
        """)
        client.command("INSERT INTO _heartbeat (note) VALUES (%(note)s)", params={"note": "dagster"})
        count = client.query("SELECT count() AS c FROM _heartbeat").first_item[0]
        logger.info(f"Heartbeat rows: {count}")

@job
def nightly_heartbeat():
    heartbeat_op()
```

---

## orchestrator/distilyze_etl/schedules.py

```python
from dagster import ScheduleDefinition
from zoneinfo import ZoneInfo
from .jobs.extract import nightly_heartbeat

# Run daily at 01:15 America/New_York

daily_heartbeat = ScheduleDefinition(
    job=nightly_heartbeat,
    cron_schedule="15 1 * * *",
    execution_timezone="America/New_York",
    name="daily_heartbeat",
)
```

---

## orchestrator/distilyze_etl/__init__.py

```python
from dagster import Definitions
from .assets.bootstrap import init_storage
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
    assets=[init_storage],
    schedules=[daily_heartbeat],
    resources=resources,
)
```

---

## docker-compose.yml — append these services

```yaml
services:
  dagster-web:
    build:
      context: ./orchestrator
    container_name: distilyze-dagster-web
    environment:
      DAGSTER_PGURL: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:${POSTGRES_PORT:-5432}/${POSTGRES_DB}
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      MINIO_REGION: ${MINIO_REGION}
      CLICKHOUSE_HOST: clickhouse
      CLICKHOUSE_HTTP_PORT: 8123
      CLICKHOUSE_USER: ${CLICKHOUSE_USER}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
      CLICKHOUSE_DB: ${CLICKHOUSE_DB}
    volumes:
      - ./orchestrator:/opt/code
      - dagster_home:/opt/dagster
    ports:
      - "3000:3000"  # Dagster UI
    command: ["bash", "-lc", "dagster-webserver -h 0.0.0.0 -p 3000 -w /opt/code/workspace.yaml"]
    depends_on:
      - postgres
      - clickhouse
      - minio
    restart: unless-stopped

  dagster-daemon:
    build:
      context: ./orchestrator
    container_name: distilyze-dagster-daemon
    environment:
      DAGSTER_PGURL: postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:${POSTGRES_PORT:-5432}/${POSTGRES_DB}
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      MINIO_REGION: ${MINIO_REGION}
      CLICKHOUSE_HOST: clickhouse
      CLICKHOUSE_HTTP_PORT: 8123
      CLICKHOUSE_USER: ${CLICKHOUSE_USER}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
      CLICKHOUSE_DB: ${CLICKHOUSE_DB}
    volumes:
      - ./orchestrator:/opt/code
      - dagster_home:/opt/dagster
    command: ["bash", "-lc", "dagster-daemon run"]
    depends_on:
      - dagster-web
    restart: unless-stopped

volumes:
  dagster_home:
```

---

## Makefile — append targets

```make
.PHONY: dagster-up dagster-down dagster-logs dagster-urls

dagster-up:
	docker compose up -d dagster-web dagster-daemon

dagster-down:
	docker compose rm -sf dagster-web dagster-daemon || true
	docker compose up -d  # keep core stack up

dagster-logs:
	docker compose logs -f --tail=200 dagster-web dagster-daemon

dagster-urls:
	@echo "Dagster UI: http://localhost:3000"
```

---

## Notes

- Scheduler is active via `dagster-daemon`. The `daily_heartbeat` schedule runs at 01:15 America/New_York.
- The heartbeat job writes a row into ClickHouse `_heartbeat`. This proves end-to-end scheduling and DB access.
- Dagster instance uses Postgres via `DAGSTER_PGURL`. Tables live under the same DB for now.
- S3 resource is configured for MinIO. You can swap to AWS later by changing env.

Ready for S1-T3.

