# Distilyze S1-T1 Provisioning

Provision MinIO, ClickHouse, and Postgres with pgvector. Keep it simple. Idempotent. Ready for S1-T2.

---

## Files

```
.
├─ .env.example
├─ docker-compose.yml
├─ postgres/
│  └─ init/
│     └─ 001_extensions.sql
├─ clickhouse/
│  └─ init/
│     └─ 001_init.sql
└─ Makefile
```

---

## .env.example

```
# Postgres
POSTGRES_USER=distilyze
POSTGRES_PASSWORD=distilyze123
POSTGRES_DB=distilyze
POSTGRES_PORT=5432

# MinIO
MINIO_ROOT_USER=distilyze
MINIO_ROOT_PASSWORD=distilyze123
MINIO_REGION=us-east-1
S3_RAW_BUCKET=distilyze-raw
S3_STAGING_BUCKET=distilyze-staging

# ClickHouse
CLICKHOUSE_USER=distilyze
CLICKHOUSE_PASSWORD=distilyze123
CLICKHOUSE_DB=distilyze
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_NATIVE_PORT=9000
```

---

## docker-compose.yml

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:16
    container_name: distilyze-postgres
    ports:
      - "${POSTGRES_PORT:-5432}:5432"
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    volumes:
      - pg_data:/var/lib/postgresql/data
      - ./postgres/init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 5s
      timeout: 3s
      retries: 20
    restart: unless-stopped

  minio:
    image: minio/minio:latest
    container_name: distilyze-minio
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      MINIO_REGION: ${MINIO_REGION}
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # Console
    volumes:
      - minio_data:/data
    restart: unless-stopped

  mc-init:
    image: minio/mc:latest
    container_name: distilyze-minio-bootstrap
    depends_on:
      - minio
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      S3_RAW_BUCKET: ${S3_RAW_BUCKET}
      S3_STAGING_BUCKET: ${S3_STAGING_BUCKET}
    entrypoint: ["/bin/sh", "-c"]
    command: >
      "set -e;
      until /usr/bin/mc alias set local http://minio:9000 \"$${MINIO_ROOT_USER}\" \"$${MINIO_ROOT_PASSWORD}\"; do
        echo 'waiting for minio...';
        sleep 2;
      done;
      /usr/bin/mc mb -p local/$${S3_RAW_BUCKET} || true;
      /usr/bin/mc mb -p local/$${S3_STAGING_BUCKET} || true;
      /usr/bin/mc anonymous set download local/$${S3_RAW_BUCKET} || true;
      /usr/bin/mc anonymous set download local/$${S3_STAGING_BUCKET} || true;
      echo 'buckets ready';"
    restart: "no"

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    container_name: distilyze-clickhouse
    environment:
      CLICKHOUSE_USER: ${CLICKHOUSE_USER}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
      CLICKHOUSE_DB: ${CLICKHOUSE_DB}
    ports:
      - "${CLICKHOUSE_HTTP_PORT:-8123}:8123"  # HTTP
      - "${CLICKHOUSE_NATIVE_PORT:-9000}:9000" # Native client
    volumes:
      - ch_data:/var/lib/clickhouse
    restart: unless-stopped

  ch-init:
    image: clickhouse/clickhouse-client:latest
    container_name: distilyze-clickhouse-bootstrap
    depends_on:
      - clickhouse
    environment:
      CLICKHOUSE_USER: ${CLICKHOUSE_USER}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD}
      CLICKHOUSE_DB: ${CLICKHOUSE_DB}
    volumes:
      - ./clickhouse/init:/init:ro
    entrypoint: ["/bin/bash", "-lc"]
    command: >
      "until clickhouse-client --host clickhouse --user $${CLICKHOUSE_USER} --password $${CLICKHOUSE_PASSWORD} --query 'SELECT 1'; do
         echo 'waiting for clickhouse...'; sleep 2; done;
       clickhouse-client --host clickhouse --user $${CLICKHOUSE_USER} --password $${CLICKHOUSE_PASSWORD} --multiquery < /init/001_init.sql;
       echo 'clickhouse ready';"
    restart: "no"

volumes:
  pg_data:
  ch_data:
  minio_data:
```

---

## postgres/init/001_extensions.sql

```sql
-- Schema and extensions
CREATE SCHEMA IF NOT EXISTS app;
CREATE EXTENSION IF NOT EXISTS vector;

-- Set default search path for the database on first boot
ALTER DATABASE distilyze SET search_path = public, app;
```

---

## clickhouse/init/001_init.sql

```sql
-- Database
CREATE DATABASE IF NOT EXISTS distilyze;

-- Optional: a lightweight heartbeat table for smoke tests
CREATE TABLE IF NOT EXISTS distilyze._heartbeat
(
  ts DateTime DEFAULT now(),
  note String
)
ENGINE = MergeTree
ORDER BY ts;
```

---

## Makefile

```make
SHELL := /bin/bash

.PHONY: up down ps logs nuke psql ch mc urls

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=100

nuke:
	docker compose down -v

psql:
	docker exec -it distilyze-postgres psql -U $$POSTGRES_USER -d $$POSTGRES_DB

ch:
	docker exec -it distilyze-clickhouse clickhouse-client -u $$CLICKHOUSE_USER --password $$CLICKHOUSE_PASSWORD -d $$CLICKHOUSE_DB

mc:
	docker exec -it distilyze-minio-bootstrap /usr/bin/mc ls local

urls:
	@echo "MinIO console: http://localhost:9001"
	@echo "MinIO S3 API: http://localhost:9000"
	@echo "ClickHouse HTTP: http://localhost:$${CLICKHOUSE_HTTP_PORT:-8123}"
	@echo "Postgres: localhost:$${POSTGRES_PORT:-5432}"
```

---

## How to run

1. Copy env file.

```
cp .env.example .env
```

2. Start services.

```
make up
```

3. Check health.

```
make ps
make logs
```

4. Open the console.

```
make urls
```

MinIO console runs on http://localhost:9001. Login with the env values.

---

## Smoke tests

Postgres.

```
make psql
-- inside psql
SELECT current_database(), current_schema();
SELECT * FROM pg_extension WHERE extname = 'vector';
\q
```

ClickHouse.

```
make ch
-- inside clickhouse-client
INSERT INTO _heartbeat(note) VALUES ('ok');
SELECT count() FROM _heartbeat;
QUIT;
```

MinIO.

```
make mc
```
You should see the raw and staging buckets.

---

## Connection strings

Postgres.

```
postgresql+psycopg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@localhost:${POSTGRES_PORT}/${POSTGRES_DB}
```

ClickHouse HTTP.

```
http://${CLICKHOUSE_USER}:${CLICKHOUSE_PASSWORD}@localhost:${CLICKHOUSE_HTTP_PORT}/${CLICKHOUSE_DB}
```

MinIO S3 (local endpoint).

```
Endpoint: http://localhost:9000
Region: ${MINIO_REGION}
Access key: ${MINIO_ROOT_USER}
Secret key: ${MINIO_ROOT_PASSWORD}
s3://$S3_RAW_BUCKET
s3://$S3_STAGING_BUCKET
```

---

## Notes

- All init steps are idempotent. You can rerun without side effects.
- Default creds are for local only. Rotate before any shared use.
- Ports can change via .env.
- This pack does not expose public endpoints.

Ready for S1-T2.

