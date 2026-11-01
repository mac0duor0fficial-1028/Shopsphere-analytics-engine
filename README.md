# ShopSphere Analytics Engine

A **Modern Data Stack** pipeline that streams an e-commerce OLTP database into Snowflake with
**Postgres CDC via Airbyte**, models it with **dbt** into a tested star schema, orchestrates it with
**Dagster**, and serves near-real-time analytics through **Dynamic Tables** and **Streamlit**.

> **"Real time" here means near-real-time.** Airbyte CDC is micro-batch (5-15 min cycles). Dynamic Tables remove
> dbt's batch latency on the hottest metrics. Sub-minute latency needs Debezium + Kafka + Snowpipe Streaming (see roadmap).

```mermaid
%%{init: {"theme": "base", "themeVariables": {"primaryTextColor": "#0F172A", "lineColor": "#64748B", "fontSize": "15px"}, "flowchart": {"curve": "basis", "nodeSpacing": 40, "rankSpacing": 55}}}%%
flowchart LR
    subgraph SRC["1. SOURCE"]
        PG[("<b>PostgreSQL</b><br/>OLTP · shop schema")]
    end

    subgraph ING["2. INGESTION"]
        AB["<b>Airbyte</b><br/>CDC · Append mode"]
    end

    subgraph SNOW["3. SNOWFLAKE"]
        RAW[("<b>RAW</b><br/>CDC change log")]

        subgraph DBT["dbt transformation"]
            STG["<b>STAGING</b><br/>dedupe by LSN"]
            INT["<b>INTERMEDIATE</b><br/>business logic"]
            MARTS[("<b>MARTS</b><br/>star schema · contracts · tests")]
            SNAP["<b>SCD2 snapshots</b><br/>history"]
        end

        DT["<b>Dynamic Tables</b><br/>opt-in · 5 min lag"]
    end

    subgraph SERVE["4. SERVING"]
        BI["<b>Streamlit dashboards</b>"]
    end

    ORCH{{"<b>Dagster</b><br/>schedules · freshness · alerts"}}

    PG -- "WAL / pgoutput" --> AB
    AB --> RAW
    RAW --> STG --> INT --> MARTS
    STG --> SNAP
    STG --> DT
    MARTS --> BI
    DT --> BI

    ORCH -. "triggers sync" .-> AB
    ORCH -. "dbt build + tests" .-> DBT
    ORCH -. "slot lag checks" .-> PG

    classDef source fill:#E0F2FE,stroke:#0284C7,stroke-width:2px,color:#0C4A6E
    classDef ingest fill:#FEF3C7,stroke:#D97706,stroke-width:2px,color:#78350F
    classDef raw fill:#CFFAFE,stroke:#0891B2,stroke-width:2px,color:#164E63
    classDef model fill:#EDE9FE,stroke:#7C3AED,stroke-width:2px,color:#3B0764
    classDef mart fill:#DCFCE7,stroke:#16A34A,stroke-width:3px,color:#14532D
    classDef rt fill:#FFE4E6,stroke:#E11D48,stroke-width:2px,stroke-dasharray:5 3,color:#881337
    classDef serve fill:#FCE7F3,stroke:#DB2777,stroke-width:2px,color:#831843
    classDef orch fill:#F1F5F9,stroke:#475569,stroke-width:2px,color:#0F172A

    class PG source
    class AB ingest
    class RAW raw
    class STG,INT,SNAP model
    class MARTS mart
    class DT rt
    class BI serve
    class ORCH orch

    style SRC fill:#F0F9FF,stroke:#BAE6FD,stroke-width:1px,color:#075985
    style ING fill:#FFFBEB,stroke:#FDE68A,stroke-width:1px,color:#92400E
    style SNOW fill:#F8FAFF,stroke:#C7D2FE,stroke-width:1px,color:#3730A3
    style DBT fill:#FAF5FF,stroke:#DDD6FE,stroke-width:1px,color:#5B21B6
    style SERVE fill:#FDF2F8,stroke:#FBCFE8,stroke-width:1px,color:#9D174D
```

## Highlights

- **Correct CDC semantics:** *Append* mode keeps the full change log; dbt dedupes by Postgres LSN, tracks deletes,
  and builds SCD2 history (ADR 001).
- **Production patterns:** key-pair auth, least-privilege RBAC, resource monitors, per-workload warehouses,
  tag-based PII masking, environment isolation (dev / prod), model contracts, source freshness SLAs,
  incremental `merge` facts with late-arrival lookback, dbt unit tests.
- **Observability:** replication-slot WAL monitoring (the real CDC failure mode), daily Postgres-vs-Snowflake
  reconciliation, Slack alerts on any failed run, runbooks for each failure class.
- **Reproducible demo:** a traffic generator exercises INSERT / UPDATE / DELETE so CDC is genuinely tested.

## Repository layout
```
shopsphere-analytics-engine/
├── Makefile                          Every operation is a make target
├── docker-compose.yml                Local Postgres (logical replication) + traffic generator
├── .env.example                      All configuration variables (copy to .env)
├── requirements/                     dbt.txt · dev.txt · bi.txt
│
├── source_db/                        STAGE 1: source system
│   ├── ddl/
│   │   ├── 01_schema.sql             OLTP tables, triggers, indexes
│   │   └── 02_cdc.sql                CDC role, REPLICA IDENTITY, publication, slot
│   └── generator/                    Faker traffic: inserts, updates, deletes, funnel events
│       ├── generator.py
│       ├── Dockerfile
│       └── tests/
│
├── infra/snowflake/sql/              STAGE 2: Snowflake foundation (idempotent)
│   ├── 01_account_objects.sql        Roles, databases, warehouses, resource monitor
│   ├── 02_grants.sql                 Least-privilege grants, schema layout
│   ├── 03_service_users.sql          Key-pair service users
│   ├── 04_governance.sql             PII tag + masking policy
│   └── 90_enable_change_tracking.sql Needed for Dynamic Tables
│
├── airbyte/                          STAGE 3: ingestion
│   ├── apply_airbyte.py              Creates source, destination, connection via API
│   └── tests/
│
├── dbt/                              STAGE 4: transformation
│   ├── dbt_project.yml · profiles.yml · packages.yml · selectors.yml
│   ├── macros/                       cdc_latest (dedupe by LSN), schema naming, PII tagging
│   ├── models/
│   │   ├── staging/                  stg_* : latest row per key, delete flags
│   │   ├── intermediate/             orders enriched, customer history, session funnel
│   │   └── marts/
│   │       ├── dim_*  fct_*          Star schema (contracts on fct_orders, dim_customers)
│   │       ├── mart_*                Revenue, LTV, cohorts, products, funnel
│   │       ├── _semantic.yml         Governed metrics (MetricFlow)
│   │       └── realtime/             Dynamic Tables (opt-in)
│   ├── snapshots/                    SCD2 history: customers, products, orders
│   └── tests/                        Business-rule tests
│
├── orchestration/                    STAGE 5: orchestration + monitoring
│   └── shopsphere_orchestration/
│       ├── assets.py                 Airbyte sync asset + dbt assets
│       ├── resources.py              Airbyte, Postgres, Snowflake resources
│       ├── monitoring.py             Slot-lag check, Postgres vs Snowflake reconciliation
│       └── definitions.py            Jobs, schedules, Slack failure sensor
│
├── bi/streamlit_app/app.py           STAGE 6: dashboards (read-only REPORTER role)
│
├── scripts/
│   ├── gen_keys.sh                   Generate key pairs for service users
│   └── snowflake_sql.py              Runs the bootstrap SQL with ${ENV} substitution
│
└── docs/
    ├── IMPLEMENTATION_RUNBOOK.md     Gated, step-by-step build guide
    ├── architecture.md
    ├── runbooks/                     slot lag · failed sync · schema drift · full re-sync
    └── adr/                          Architecture decision records
```

> **Implementing it?** Follow the gated, step-by-step [Implementation Runbook](docs/IMPLEMENTATION_RUNBOOK.md).

## Quickstart (local source + your Snowflake account)

Prerequisites: Docker, Python 3.10+, OpenSSL, a Snowflake account (**Enterprise edition** for tag-based masking;
Standard works if you skip `04_governance.sql` and the PII post-hooks), and Airbyte via
[`abctl local install`](https://docs.airbyte.com/platform/using-airbyte/getting-started/oss-quickstart).

```bash
cp .env.example .env                       # fill in values
pip install -r requirements/dev.txt

# 1. Source database with logical replication, then CDC role/publication/slot
make up && make cdc-setup

# 2. Snowflake: keys -> bootstrap
./scripts/gen_keys.sh                      # paste the printed *_RSA_PUBLIC_KEY lines into .env
make snowflake-bootstrap

# 3. Airbyte: create credentials in the UI (abctl local credentials), set AIRBYTE_* in .env, then
make airbyte-plan                          # review (secrets redacted)
make airbyte-apply                         # prints AIRBYTE_CONNECTION_ID -> add to .env

# 4. Seed data, first sync, build models
make seed
#    run one sync (Airbyte UI or Dagster), then:
make dbt-deps && make dbt-build

# 5. Orchestrate and visualise
make dagster-dev                           # http://localhost:3000 (enable the schedules)
make generate &                            # continuous traffic
make bi                                    # Streamlit
```

Before any production build, run the single local quality gate: `make check` (lint, `dbt parse`, unit tests).
There is no CI service to maintain; deploys are `make dbt-build DBT_TARGET=prod`.

Optional near-real-time layer: `make enable-change-tracking`, then
`dbt build --vars '{enable_realtime: true}' --select tag:realtime`.

## Validate end to end

| Test | Expected |
|---|---|
| Insert / update / delete a row in Postgres, run a sync | row appears, changes, or is flagged `is_deleted` in `fct_orders` |
| `ALTER TABLE shop.orders ADD COLUMN note text` | propagates to RAW; models keep working; follow the schema-drift runbook for marts |
| Stop Airbyte for an hour, resume | catches up, no gaps or duplicates in marts (dedupe by LSN) |
| `SESSIONS_PER_SEC=60`, scale generator containers | freshness holds; `cdc_health_job` shows bounded retained WAL |
| `dbt build --full-refresh` | idempotent, identical marts |
| Rotate a service key | update `ALTER USER ... SET RSA_PUBLIC_KEY_2`, switch, then unset the old key; no downtime |



## Cost and performance notes

- Warehouses auto-suspend at 60s; Airbyte on `XSMALL`, dbt on `SMALL`; a resource monitor suspends at 90% of the quota.
- Staging views re-scan the CDC log each build. That is fine at portfolio scale; at production scale convert hot
  staging models to incremental and apply retention to `RAW` (change log) tables.
- Airbyte/Dagster cadence is the main cost lever: every sync wakes `LOADING_WH` and `TRANSFORM_WH`.

## Roadmap

Debezium + Kafka + Snowpipe Streaming for sub-minute latency; Terraform port (ADR 002); GDPR erasure workflow that
purges RAW and facts; reverse ETL; ML features (churn, recommendations).

## License

MIT
