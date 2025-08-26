# Distilyze Stack

This document is a human friendly view of `stack.yaml`. It summarizes flows and shows how components connect. Lists and field names live in the YAML as the single source of truth.

## High level flow

```mermaid
flowchart LR
  A[GL API + Bank + Card + AP Invoices] --> B[Ingest Raw]
  B --> C[Normalize Entities]
  C --> D[Enrich and Stage]
  D --> E[Ledger Auto Map]
  E --> F[Controls and Anomalies]
  F --> G[Accruals and Amortization]
  E --> H[Forecasts]
  E --> I[Ops Dashboards]
  I --> J[Review Queue]
  J --> K[Feedback Loop]
  K --> L[Fine Tuning Loop]
  L --> E
```

## System architecture

```mermaid
graph LR
  subgraph Orchestration
    DAG[Dagster]
  end
  subgraph Storage
    M[MinIO Parquet]
    WH[ClickHouse]
    PG[Postgres + pgvector]
    FS[Feast]
  end
  subgraph Transform_Quality
    DBT[dbt Core]
    GE[Great Expectations]
    OL[OpenLineage + Marquez]
  end
  subgraph Serving
    API[FastAPI]
    INF[vLLM + BentoML/Triton]
    BI[Superset/Metabase]
    AUTH[Keycloak]
    SECV[Vault]
  end
  subgraph MLOps
    MLW[MLflow]
    MON[Evidently/Arize]
  end
  DAG --> M
  M --> DBT --> WH
  WH --> BI
  PG --> API
  FS --> INF
  API --> INF
  OL -.-> DAG
  GE -.-> DAG
```

## Controls at a glance

- Autopost only when confidence is high, amount is small, and account is non sensitive.
- Everything else routes to review with stored reasons.
- Every change is versioned to keep audit trails.
- Drift monitors watch schema, confidence, and class mix.

## Copilot boundaries

```mermaid
sequenceDiagram
  participant User
  participant Copilot
  participant SQL as Read only SQL
  participant Docs as Vector Search
  participant Ledger as Proposed Journal

  User->>Copilot: Explain this anomaly
  Copilot->>SQL: Query safe template
  SQL-->>Copilot: Aggregates and rows
  Copilot->>Docs: Retrieve contract and policy
  Docs-->>Copilot: Snippets
  Copilot-->>User: Why it tripped and next step
  User->>Copilot: Propose month end entry
  Copilot->>Ledger: Write to proposed_journal
  Ledger-->>User: Link for review
```

## Timeline summary

- Weeks 1 to 2 ship ingest, normalization, marts, dashboards, and basic rules.
- Weeks 3 to 4 add LLM few shot mapping, OCR, feedback UI, and GBM mapping with tie break by route.
- Week 5 and after add accruals, forecasts, copilot, QLoRA, and drift monitors.
