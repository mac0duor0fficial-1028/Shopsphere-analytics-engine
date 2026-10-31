# ShopSphere Analytics Engine: Implementation Runbook

Follow this top to bottom. Every phase ends with a **GATE**: verification commands and the result you must
see before moving on. If a gate fails, fix it using the Troubleshooting matrix (section 14) before continuing;
later phases assume earlier gates passed.

**Time:** about 3-4 hours for a first end-to-end run (local source + Snowflake trial/dev account).
**Conventions:** `$` = run on your workstation. `PG>` = run in psql against the source. `SF>` = run in a
Snowflake worksheet/SnowSQL. Paths are relative to the repository root unless stated.

---

## 0. Architecture in one minute

```
Postgres (shop schema) --WAL/pgoutput--> Airbyte --> SHOPSPHERE_RAW.AIRBYTE (CDC change log)
   dbt: STAGING (dedupe by LSN) -> INTERMEDIATE -> MARTS (+ SNAPSHOTS)      Dagster orchestrates + monitors
   Optional: Dynamic Tables (5 min lag)                                     Streamlit reads MARTS (REPORTER role)
```

Order of construction (each phase depends on the previous): **A** workstation, **B** source + CDC,
**C** Snowflake, **D** Airbyte, **E** first sync, **F** dbt, **G** Dagster, **H** near-real-time (optional),
**I** BI, **J** local quality gate, **K** acceptance tests, **L** production hardening.

---

## 1. Prerequisites (complete before Phase A)

| # | Requirement | How to check |
|---|---|---|
| 1 | Docker Desktop / Docker Engine with Compose v2 | `docker compose version` |
| 2 | Python 3.10-3.12 | `python3 --version` |
| 3 | OpenSSL | `openssl version` |
| 4 | Snowflake account, user with **ACCOUNTADMIN** | Log in to Snowsight; run `SELECT CURRENT_ROLE();` after `USE ROLE ACCOUNTADMIN;` |
| 5 | Snowflake edition: **Enterprise or higher** for tag-based masking (Standard: see step C5) | Snowsight > Admin > Accounts shows the edition |
| 6 | Snowflake account identifier in `orgname-accountname` form | Snowsight > account menu > "Copy account identifier" |
| 7 | Airbyte via `abctl` (needs ~4 CPU / 8 GB free for Docker) | `abctl version` |

**Decisions to lock now** (defaults in brackets): Airbyte deployment [self-hosted `abctl`], Snowflake cloud
[whatever your account uses], orchestrator [Dagster], monthly credit budget [100 credits], SLA [core marts
<= 15 min fresh, >= 99% pipeline success].

> If your Snowflake admin user enforces **MFA / blocks password login**, use key-pair auth for the bootstrap script
> (`SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH` in `.env`). Passwords alone will fail.

---

## Phase A: Workstation setup (10 min)

```bash
$ cd shopsphere-analytics-engine
$ python3 -m venv .venv && source .venv/bin/activate
$ pip install -r requirements/dev.txt          # dbt, sqlfluff, pytest, Dagster (editable), connectors
$ pip install -r requirements/bi.txt           # Streamlit (needed in Phase I)
$ cp .env.example .env
```

Edit `.env`. Rules (they prevent most first-run failures):

- **No quotes and no spaces** in values (the Makefile and `source .env` both read this file).
- **Key paths must be absolute** (dbt runs from `dbt/`, Dagster from `orchestration/`, so relative paths break).
- Use strong values for `POSTGRES_PASSWORD` and `AIRBYTE_CDC_PASSWORD`; URL-encode special characters inside `PG_DSN`.
- Fill what you know now: `POSTGRES_*`, `AIRBYTE_CDC_PASSWORD`, `PG_DSN`, `SNOWFLAKE_ACCOUNT`, admin credentials,
  `SNOWFLAKE_MONTHLY_CREDIT_QUOTA`, `DBT_DEV_SCHEMA_SUFFIX` (your short name, lowercase letters/digits).
  Leave the `AIRBYTE_*` IDs and public keys for later phases.

**GATE A**

```bash
$ make help                                     # lists targets
$ pytest -q source_db/generator/tests airbyte/tests orchestration/tests/test_monitoring.py
# expect: 13 passed
```

---

## Phase B: Source Postgres and CDC (20 min)

### B1. Start Postgres with logical replication

```bash
$ make up
```

This runs Postgres 16 with `wal_level=logical`, `max_replication_slots=10`, `max_wal_senders=10`,
`max_slot_wal_keep_size=4GB`, and creates the `shop` schema from `source_db/ddl/01_schema.sql` on first start.

### B2. Create the CDC role, publication and slot

```bash
$ make cdc-setup
```

Idempotent: safe to repeat. It creates role `airbyte_cdc` (LOGIN, REPLICATION), sets `REPLICA IDENTITY FULL`
on six tables, creates publication `shopsphere_pub`, and slot `airbyte_slot` (pgoutput).

### B3. Seed realistic data

```bash
$ make seed                                     # runs the generator for 30 seconds
```

**GATE B**: connect with `docker compose exec postgres psql -U shop_admin -d shopsphere`

```sql
PG> SHOW wal_level;                              -- logical
PG> SELECT slot_name, plugin, slot_type, wal_status FROM pg_replication_slots;
--  airbyte_slot | pgoutput | logical | reserved
PG> SELECT pubname FROM pg_publication;          -- shopsphere_pub
PG> SELECT count(*) FROM pg_publication_tables WHERE pubname='shopsphere_pub';   -- 7
PG> SELECT relname, relreplident FROM pg_class
    WHERE relnamespace='shop'::regnamespace AND relkind='r' ORDER BY 1;          -- 'f' for 6 tables, events = 'd'
PG> SELECT (SELECT count(*) FROM shop.customers) c, (SELECT count(*) FROM shop.products) p,
           (SELECT count(*) FROM shop.orders) o, (SELECT count(*) FROM shop.events) e;   -- all > 0
```

Record the order count; you will compare it to Snowflake in Phase E.

> **Production source (RDS/Cloud SQL) instead of Docker:** skip `make up`/`seed`. Apply `01_schema.sql` only if the
> tables do not exist, set the parameter group (`rds.logical_replication=1`, `max_slot_wal_keep_size`), run
> `GRANT rds_replication TO airbyte_cdc;` after `02_cdc.sql`, restart, and make Postgres reachable from Airbyte.

---

## Phase C: Snowflake foundation (30 min)

### C1. Generate service-user key pairs

```bash
$ ./scripts/gen_keys.sh
```

You will be prompted for a passphrase for each key. **Use a passphrase and record it**: put it in `.env`
(`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` for dbt, `AIRBYTE_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`, `BI_PRIVATE_KEY_PASSPHRASE`).
The script prints three lines: `AIRBYTE_RSA_PUBLIC_KEY=...`, `DBT_RSA_PUBLIC_KEY=...`, `BI_RSA_PUBLIC_KEY=...`.
Paste them into `.env`. Set the three `*_PRIVATE_KEY_PATH` values to the absolute paths of the `.p8` files in `secrets/`.

### C2. Bootstrap Snowflake objects

```bash
$ make snowflake-bootstrap
```

Runs, in order, as ACCOUNTADMIN: `01_account_objects.sql` (roles, databases, three warehouses, resource monitor),
`02_grants.sql` (least-privilege grants, RAW/ANALYTICS schemas), `03_service_users.sql` (key-pair service users),
`04_governance.sql` (PII tag + masking policy). Each statement prints `ok`; a failure stops the run with the
Snowflake error. All statements are idempotent, so fix the cause and re-run the whole target.

### C3. Set resource-monitor notifications (one-time)

```sql
SF> USE ROLE ACCOUNTADMIN;
SF> ALTER RESOURCE MONITOR SHOPSPHERE_RM SET NOTIFY_USERS = (YOUR_ADMIN_USER);
```

The user must have a verified email, otherwise the 75% notify trigger has no recipient.

### C4. Verify

```sql
SF> SHOW ROLES;                                                    -- LOADER, TRANSFORMER, REPORTER, ANALYST, PII_READER present
SF> SHOW WAREHOUSES;                                               -- LOADING_WH XSMALL, TRANSFORM_WH SMALL, BI_WH XSMALL; auto_suspend 60; resource_monitor SHOPSPHERE_RM
SF> SHOW DATABASES LIKE 'SHOPSPHERE%';                             -- RAW, ANALYTICS, ANALYTICS_DEV
SF> SHOW SCHEMAS IN DATABASE SHOPSPHERE_RAW;                       -- AIRBYTE, AIRBYTE_INTERNAL
SF> SHOW SCHEMAS IN DATABASE SHOPSPHERE_ANALYTICS;                 -- STAGING, INTERMEDIATE, MARTS, SNAPSHOTS, GOVERNANCE
SF> SHOW USERS LIKE 'SVC_%';                                       -- 3 users, type SERVICE
SF> DESC USER SVC_DBT;                                             -- RSA_PUBLIC_KEY_FP populated
SF> SHOW GRANTS TO ROLE REPORTER;                                  -- only MARTS + BI_WH + database usage
```

### C5. Standard edition fallback (no tag-based masking)

If `04_governance.sql` fails with an unsupported-feature error:

1. In the `Makefile`, remove `infra/snowflake/sql/04_governance.sql` from the `snowflake-bootstrap` target.
2. Remove the `post_hook` from `dbt/models/staging/stg_customers.sql` and `dbt/models/marts/dim_customers.sql`,
   and from `snap_customers` in `dbt/snapshots/snapshots.yml`.
3. Restrict PII another way (for example, do not grant `ANALYST` the `STAGING` schema). Document the gap.

**GATE C** (proves key-pair auth works for the dbt service user, which is what every later phase relies on):

```bash
$ cd dbt && set -a && source ../.env && set +a && dbt deps && dbt debug ; cd ..
# expect: "Connection test: [OK connection ok]"  and  "All checks passed!"
```

---

## Phase D: Airbyte (30 min)

### D1. Install Airbyte and get API credentials

```bash
$ abctl local install                            # first run takes several minutes
$ abctl local credentials                        # shows UI password and API client id/secret
```

Open http://localhost:8000, create the admin account if prompted, and put the API client id/secret in `.env`
(`AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`). Find the workspace id in the UI URL
(`/workspaces/<id>/...`) and set `AIRBYTE_WORKSPACE_ID`.

Optional API check:
```bash
$ set -a; source .env; set +a
$ TOKEN=$(curl -s -X POST "$AIRBYTE_API_URL/applications/token" -H 'Content-Type: application/json' \
    -d "{\"client_id\":\"$AIRBYTE_CLIENT_ID\",\"client_secret\":\"$AIRBYTE_CLIENT_SECRET\",\"grant-type\":\"client_credentials\"}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
$ curl -s -H "Authorization: Bearer $TOKEN" "$AIRBYTE_API_URL/workspaces"     # lists your workspace id
```

### D2. Make Postgres reachable from Airbyte

Airbyte runs inside containers. On **Docker Desktop** `AIRBYTE_SOURCE_PG_HOST=host.docker.internal` works. On
**Linux**, use the host's LAN IP (Postgres already publishes port 5432). Set `AIRBYTE_SOURCE_PG_HOST` in `.env`.

### D3. Preview, then apply

```bash
$ make airbyte-plan                              # prints source/destination/connection JSON, secrets redacted
$ make airbyte-apply                             # creates or updates by name (safe to re-run)
```

The apply output ends with `Set AIRBYTE_CONNECTION_ID=<uuid> in your .env`. Do that now.

What gets created: source `shopsphere-postgres-cdc` (CDC, pgoutput, slot `airbyte_slot`, publication `shopsphere_pub`,
commit LSN only **after** loading to the destination), destination `shopsphere-snowflake-raw` (key-pair, role
`LOADER`, `LOADING_WH`, database `SHOPSPHERE_RAW`, schema `AIRBYTE`, raw schema `AIRBYTE_INTERNAL`), and connection
`shopsphere-postgres-to-snowflake` (7 streams, **Incremental | Append**, `propagate_columns`, schedule from
`AIRBYTE_SCHEDULE`, default `manual` so Dagster owns timing).

If the API rejects a field, the script prints Airbyte's message. Connector specs evolve: compare the field with the
form in the Airbyte UI for your version, adjust `apply_airbyte.py`, and re-run. Test the source and destination
with the UI "Test connection" buttons.

**GATE D**: in the Airbyte UI the connection shows 7 streams, all "Incremental | Append", status Active; source and
destination tests pass.

---

## Phase E: First sync and raw-layer validation (20 min)

### E1. Run the first sync

Airbyte UI > connection > **Sync now** (or, once Dagster runs in Phase G, materialise `airbyte_cdc_sync`).
The first run does an initial snapshot, then switches to reading the WAL. Wait until it reports success.

### E2. Validate the raw layer

```sql
SF> USE ROLE TRANSFORMER; USE WAREHOUSE TRANSFORM_WH;
SF> SELECT COUNT(*) AS n, COUNT(_AB_CDC_LSN) AS with_lsn FROM SHOPSPHERE_RAW.AIRBYTE.ORDERS;
SF> DESC TABLE SHOPSPHERE_RAW.AIRBYTE.ORDERS;
```

`DESC` must show your business columns plus `_AB_CDC_LSN`, `_AB_CDC_UPDATED_AT`, `_AB_CDC_DELETED_AT`,
`_AIRBYTE_RAW_ID`, `_AIRBYTE_EXTRACTED_AT`, `_AIRBYTE_META`. All six `models/staging` files assume exactly these names.

**If table or column names are lowercase** (quoted identifiers) or the tables live in a different schema, fix it
in `dbt/models/staging/_stg__sources.yml` (`schema:`, and add `identifier:` or `quoting:` per table). Do this now.

**GATE E**

- `ORDERS` row count is >= the Postgres order count you recorded (the raw layer is a change log, so it can be larger).
- `SELECT COUNT(*) FROM SHOPSPHERE_RAW.AIRBYTE.EVENTS;` equals `SELECT count(*) FROM shop.events` (append-only table).
- `_AB_CDC_LSN` is populated (`with_lsn` = `n`, or nearly so).
- Postgres: `SELECT active, wal_status FROM pg_replication_slots;` shows `reserved`; retained WAL is small
  after the sync (query in runbook `replication-slot-lag.md`).

---

## Phase F: dbt (40 min)

### F1. Install packages and run the development build

```bash
$ make dbt-deps
$ set -a; source .env; set +a
$ cd dbt
$ dbt source freshness                           # should be pass/warn right after a sync
$ dbt build                                      # DBT_TARGET=dev: writes to SHOPSPHERE_ANALYTICS_DEV, schema dbt_<suffix>_*
```

`dbt build` runs models, snapshots, tests and the unit test in dependency order. Expect:

- Staging views, intermediate views, marts tables, `snap_*` snapshots built.
- Contracts on `fct_orders` and `dim_customers` enforced at build time.
- Some `warn`-severity tests may fire on a fresh dataset (for example total-vs-items on an in-flight order). `error`
  failures must be fixed before continuing.

### F2. Spot-check results

```sql
SF> USE ROLE TRANSFORMER;
SF> SELECT COUNT(*) FROM SHOPSPHERE_ANALYTICS_DEV.DBT_<SUFFIX>_STAGING.STG_ORDERS WHERE NOT IS_DELETED;   -- ~ Postgres order count
SF> SELECT * FROM SHOPSPHERE_ANALYTICS_DEV.DBT_<SUFFIX>_MARTS.MART_DAILY_REVENUE ORDER BY ORDER_DATE DESC LIMIT 10;
SF> SELECT * FROM SHOPSPHERE_ANALYTICS_DEV.DBT_<SUFFIX>_MARTS.MART_FUNNEL ORDER BY SESSION_DATE DESC LIMIT 5;
```

### F3. Promote to production

```bash
$ make dbt-build DBT_TARGET=prod                 # builds SHOPSPHERE_ANALYTICS.{STAGING,INTERMEDIATE,MARTS,SNAPSHOTS}
$ cd dbt && dbt docs generate && cd ..
```

Production uses the `prod` target; schema names are exactly `STAGING`, `MARTS`, etc. (macro `generate_schema_name`).
Re-apply REPORTER/ANALYST grants if you created tables before running `02_grants.sql` (future grants cover new objects):
```sql
SF> USE ROLE SECURITYADMIN;
SF> GRANT SELECT ON ALL TABLES IN SCHEMA SHOPSPHERE_ANALYTICS.MARTS TO ROLE REPORTER;
```

### F4. If the build fails

| Failure | Cause and fix |
|---|---|
| `Object 'SHOPSPHERE_RAW.AIRBYTE.ORDERS' does not exist or not authorized` | Sync not finished, wrong schema/case (see E2), or missing grant: `SHOW GRANTS TO ROLE TRANSFORMER;` then re-run `make snowflake-bootstrap` |
| `contract ... data type mismatch` | Read the diff dbt prints; change `data_type` in `_marts__models.yml` and the matching cast in the model |
| `Insufficient privileges ... CREATE SCHEMA` | `02_grants.sql` was not applied for that database |
| Tag/masking error on post-hook | Edition does not support tag masking: follow C5; or `04_governance.sql` was skipped |
| Unit test fails | Compare actual vs expected rows in the output; the test is in `_stg__models.yml` |

**GATE F**: `dbt build` finishes with **0 errors** on prod; `SELECT COUNT(*) FROM SHOPSPHERE_ANALYTICS.MARTS.FCT_ORDERS WHERE NOT IS_DELETED;`
is within 0.5% of the Postgres order count (ignore the last ~45 minutes of orders still in flight).

Masking check:
```sql
SF> USE ROLE REPORTER; SELECT EMAIL FROM SHOPSPHERE_ANALYTICS.MARTS.DIM_CUSTOMERS LIMIT 1;   -- ***MASKED***
SF> USE ROLE TRANSFORMER; SELECT EMAIL FROM SHOPSPHERE_ANALYTICS.MARTS.DIM_CUSTOMERS LIMIT 1; -- real value
```

---

## Phase G: Orchestration with Dagster (30 min)

### G1. Environment

Dagster reads these from the environment: `AIRBYTE_API_URL`, `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`,
`AIRBYTE_CONNECTION_ID`, `PG_DSN`, `SNOWFLAKE_ACCOUNT`, `DBT_SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`,
`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`, plus optional `SLACK_WEBHOOK_URL`, `CDC_MAX_RETAINED_WAL_MB`, `RECON_TOLERANCE_PCT`.
Set `DBT_TARGET=prod` in `.env` for orchestrated runs. `make dagster-dev` exports `.env` automatically.

### G2. Start and wire up

```bash
$ make dbt-deps                                  # required: the dbt manifest is built from the project
$ make dagster-dev                               # http://localhost:3000
```

In the UI:

1. **Assets**: you should see group `ingestion` (`shopsphere_raw/*`, seven assets from one multi-asset
   `airbyte_cdc_sync`) feeding the dbt models (lineage connects raw to staging).
2. Materialise `airbyte_cdc_sync` once (triggers and waits for an Airbyte sync; a job already running is joined, not duplicated).
3. Run the job `hot_pipeline_job` (sync + `dbt source freshness` + `dbt build` of tag `hot`).
4. **Automation**: turn on the schedules: `hot_pipeline_job` (every 15 min), `cdc_health_job` (every 5 min),
   `daily_full_job` (03:00 UTC), `reconciliation_job` (04:30 UTC), and the sensor `slack_on_failure`.

### G3. Alerting check

Set `SLACK_WEBHOOK_URL`, restart, then force a failure (for example run `reconciliation_job` with a wrong `PG_DSN`
or temporarily set `CDC_MAX_RETAINED_WAL_MB=0`). A message must arrive in Slack. Restore the values.

**GATE G**

- `hot_pipeline_job` succeeds end to end.
- `cdc_health_job` succeeds (slot healthy).
- `reconciliation_job` succeeds (drift within `RECON_TOLERANCE_PCT`, default 0.5%) once the pipeline has run at least once
  and the last 45 minutes of data have synced.

> Run Dagster as a long-lived service for production (containers or a VM with `dagster-webserver` and
> `dagster-daemon`, `DAGSTER_HOME` on persistent storage). Run `dbt deps && dbt parse` during image build so the
> manifest exists without `dagster dev`.

---

## Phase H: Near-real-time layer (optional, 15 min)

Dynamic Tables read the deduped CDC log directly so hot metrics do not wait for a dbt run. Freshness is still bounded
by the Airbyte sync interval plus `target_lag` (5 min).

```bash
$ make enable-change-tracking                    # run AFTER the first sync; repeat if Airbyte recreates tables
$ cd dbt && dbt build --vars '{enable_realtime: true}' --select tag:realtime
```

```sql
SF> SHOW DYNAMIC TABLES IN SCHEMA SHOPSPHERE_ANALYTICS.MARTS;     -- refresh_mode: INCREMENTAL or FULL (both valid), scheduling_state: RUNNING
SF> SELECT * FROM SHOPSPHERE_ANALYTICS.MARTS.RT_REVENUE_BY_MINUTE ORDER BY MINUTE_TS DESC LIMIT 5;
```

Note: the Dagster dbt manifest is parsed with `enable_realtime=false`, so Dynamic Tables are built outside Dagster
unless you set the var in `dbt_project.yml`. Decide explicitly which you want and keep it consistent.

**GATE H** (skip if not used): the table refreshes (see `SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())`)
and shows a revenue minute created after your last Airbyte sync.

---

## Phase I: BI (10 min)

```bash
$ set -a; source .env; set +a
$ make bi                                        # Streamlit on http://localhost:8501
```

The app connects as `SVC_BI` with role `REPORTER` on `BI_WH` and reads only `SHOPSPHERE_ANALYTICS.MARTS`.
Tabs: Executive KPIs, Sales & Products, Customers & Cohorts, Funnel, Inventory & Ops, Pipeline health.

**GATE I**: all six tabs render with data; emails never appear (REPORTER cannot see PII).

---

## Phase J: Local quality gate (5 min)

There is no CI service to maintain. One command runs every check and is the only thing you need before a production build:

```bash
$ make check          # dbt deps, black, yamllint, sqlfluff, dbt parse, unit tests
```

Habit: run `make check` after every change and before `make dbt-build DBT_TARGET=prod`. Optionally add it as a git
pre-push hook: `printf '#!/bin/sh\nmake check\n' > .git/hooks/pre-push && chmod +x .git/hooks/pre-push`.

**GATE J**: `make check` ends with `All checks passed.`

---

## Phase K: Acceptance tests (60 min)

Stop the generator first (`docker compose --profile generator down` if running) so you control the changes. Use a
single sync + `dbt build` between each step (Dagster `hot_pipeline_job`).

### K1. INSERT / UPDATE / DELETE propagate

```bash
$ docker compose exec -T postgres psql -U shop_admin -d shopsphere -At -c \
  "INSERT INTO shop.orders (customer_id,status,channel,total_amount) VALUES (1,'pending','web',10.00) RETURNING order_id;"
# note the id as <ID>.  run hot_pipeline_job.
```
```sql
SF> SELECT STATUS, IS_DELETED FROM SHOPSPHERE_ANALYTICS.MARTS.FCT_ORDERS WHERE ORDER_ID = <ID>;      -- pending, FALSE
```
```bash
$ docker compose exec -T postgres psql -U shop_admin -d shopsphere -c "UPDATE shop.orders SET status='paid' WHERE order_id=<ID>;"
# run hot_pipeline_job
```
```sql
SF> SELECT STATUS FROM SHOPSPHERE_ANALYTICS.MARTS.FCT_ORDERS WHERE ORDER_ID = <ID>;                  -- paid
SF> SELECT STATUS, _AB_CDC_LSN, _AB_CDC_DELETED_AT FROM SHOPSPHERE_RAW.AIRBYTE.ORDERS WHERE ORDER_ID = <ID> ORDER BY _AB_CDC_LSN;  -- 2+ change rows
```
```bash
$ docker compose exec -T postgres psql -U shop_admin -d shopsphere -c "DELETE FROM shop.orders WHERE order_id=<ID>;"
# run hot_pipeline_job
```
```sql
SF> SELECT IS_DELETED FROM SHOPSPHERE_ANALYTICS.MARTS.FCT_ORDERS WHERE ORDER_ID = <ID>;              -- TRUE
SF> SELECT COUNT(*) FROM SHOPSPHERE_ANALYTICS.SNAPSHOTS.SNAP_ORDERS WHERE ORDER_ID = <ID>;           -- >= 2 versions (SCD2), after daily_full_job
```

**Expected:** all three changes arrive within the SLA (one sync + one dbt run), the delete is a soft delete in facts,
and the raw table keeps the full history.

### K2. Schema drift

```bash
$ docker compose exec -T postgres psql -U shop_admin -d shopsphere -c "ALTER TABLE shop.orders ADD COLUMN note TEXT;"
```
Run a sync (if the column does not appear in RAW, use "Refresh source schema" on the connection). Confirm
`DESC TABLE SHOPSPHERE_RAW.AIRBYTE.ORDERS` lists `NOTE`, and that `dbt build` still passes (staging selects explicit columns).
Follow `docs/runbooks/schema-drift.md` to expose the column downstream. Revert with `ALTER TABLE shop.orders DROP COLUMN note;`
only after pausing the connection (a dropped column is a breaking change).

### K3. Outage and catch-up

1. Disable the `hot_pipeline_job` schedule for 60 minutes while running `make generate` in another terminal.
2. Watch WAL growth: `SELECT pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots;`
3. Re-enable and run `hot_pipeline_job`. WAL retention must fall back to near zero.
4. Run `reconciliation_job`: **no** drift, and `SELECT COUNT(*), COUNT(DISTINCT ORDER_ID) FROM ...FCT_ORDERS` are equal (no duplicates).

### K4. Load test

```bash
$ SESSIONS_PER_SEC=40 docker compose --profile generator up --scale generator=3 -d generator
```
(each container targets 40 sessions/s; about 15% become orders). Confirm the pipeline keeps up: `dbt source freshness`
passes, retained WAL stays bounded, `hot_pipeline_job` duration is under the schedule interval.

### K5. Idempotent rebuild

```sql
SF> SELECT COUNT(*), SUM(TOTAL_AMOUNT) FROM SHOPSPHERE_ANALYTICS.MARTS.FCT_ORDERS WHERE NOT IS_DELETED;   -- record A
```
```bash
$ cd dbt && DBT_TARGET=prod dbt build --full-refresh --exclude tag:realtime
```
Re-run the query: results must equal A (if no new data arrived in between).

### K6. Key rotation without downtime

```sql
SF> ALTER USER SVC_DBT SET RSA_PUBLIC_KEY_2 = '<new public key, one line>';
```
Switch `SNOWFLAKE_PRIVATE_KEY_PATH` to the new key, confirm `dbt debug` passes, then
`ALTER USER SVC_DBT UNSET RSA_PUBLIC_KEY;`. Repeat for `SVC_AIRBYTE` (update the Airbyte destination through
`make airbyte-apply`) and `SVC_BI`.

### K7. Security checks

```sql
SF> USE ROLE REPORTER; SELECT * FROM SHOPSPHERE_ANALYTICS.STAGING.STG_ORDERS LIMIT 1;   -- must FAIL (no access)
SF> USE ROLE ANALYST;  SELECT EMAIL FROM SHOPSPHERE_ANALYTICS.STAGING.STG_CUSTOMERS LIMIT 1;  -- ***MASKED***
SF> USE ROLE REPORTER; SELECT * FROM SHOPSPHERE_RAW.AIRBYTE.ORDERS LIMIT 1;             -- must FAIL
```

**GATE K**: K1-K7 all behave as stated. Record the results (screenshots, query output) in `docs/` for your portfolio.

---

## Phase L: Production hardening and go-live

Complete before pointing real data at it.

**Secrets and access**
- Move `.env` values into a secrets manager (AWS Secrets Manager, Vault, your cloud provider's equivalent). Never commit `.env` or `secrets/`.
- Snowflake **network policy** for the service users (restrict to your Airbyte/Dagster egress IPs):
  ```sql
  SF> USE ROLE ACCOUNTADMIN;
  SF> CREATE NETWORK POLICY SHOPSPHERE_SERVICES ALLOWED_IP_LIST = ('<ip1>/32','<ip2>/32');
  SF> ALTER USER SVC_AIRBYTE SET NETWORK_POLICY = SHOPSPHERE_SERVICES;   -- repeat for SVC_DBT, SVC_BI
  ```
- Postgres: require TLS (`AIRBYTE_SOURCE_PG_SSL_MODE=require` and re-run `make airbyte-apply`), restrict `airbyte_cdc` by `pg_hba.conf`/security group.

**Source safety**
- Confirm `max_slot_wal_keep_size` is set. Add cloud alarms (RDS: `OldestReplicationSlotLag`, `TransactionLogsDiskUsage`).
- Enable an Airbyte heartbeat or a periodic write to a heartbeat table if the source is low-traffic.

**Snowflake cost and storage**
- Lower Time Travel on the change log: `ALTER SCHEMA SHOPSPHERE_RAW.AIRBYTE_INTERNAL SET DATA_RETENTION_TIME_IN_DAYS = 1;`
  (and for `AIRBYTE` if your recovery policy allows).
- Review `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` weekly; adjust `AIRBYTE_SCHEDULE`/Dagster cron to the real SLA.
- At scale, convert hot staging views to incremental models and set a retention policy for the RAW change log.

**Orchestration**
- Run Dagster as a service with persistent `DAGSTER_HOME`; run `dbt deps && dbt parse` in the image build.
- Confirm Slack/PagerDuty routing and a named on-call owner for each runbook in `docs/runbooks/`.

**Governance**
- Review who holds `PII_READER`. Document data owners; publish dbt docs (`make dbt-docs`, then `cd dbt && dbt docs serve`).

### Go-live checklist

| Item | Done |
|---|---|
| Gates A-K passed on production objects | |
| Secrets in a manager; none in Git history (`git log -p | grep -i "BEGIN.*PRIVATE"` is empty) | |
| Network policies and TLS enabled | |
| Resource monitor has notify recipients | |
| Slot-lag alarm and `cdc_health_job` alerting verified | |
| Daily reconciliation green for 3 consecutive days | |
| Runbooks reviewed by the on-call engineer | |
| Rollback plan (section 15) understood | |

---

## 13. Daily and weekly operations

| Cadence | Task |
|---|---|
| Continuous | Slack alerts from `slack_on_failure` |
| Daily | Dagster: `daily_full_job` and `reconciliation_job` green; Postgres retained WAL small |
| Weekly | Warehouse credits vs quota; slowest dbt models (`dbt run --select ... ` timings / query history); open dbt test warnings |
| Monthly | Review roles and grants; rotate keys quarterly; review RAW storage growth |
| On change | Follow `docs/runbooks/schema-drift.md` for any source schema change |

---

## 14. Troubleshooting matrix

| Symptom | Likely cause | Fix |
|---|---|---|
| `dbt debug`: JWT token is invalid | Public key not set, wrong user, or account identifier wrong | `DESC USER SVC_DBT` shows `RSA_PUBLIC_KEY_FP`; re-paste key (one line, no header); use `orgname-accountname`; check passphrase |
| `make snowflake-bootstrap`: authentication/MFA error | Admin password blocked by MFA | Use `SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH` |
| `Missing environment variables: ...` from the SQL runner | `.env` value empty | Fill it; the runner refuses to emit broken SQL |
| Airbyte source test: cannot connect | Host unreachable from containers | Use host IP on Linux; confirm port published; check `AIRBYTE_SOURCE_PG_HOST` |
| Airbyte: replication slot/publication does not exist | `make cdc-setup` not run, or DB recreated | Re-run it; re-check Gate B |
| Initial sync stuck / times out waiting | Large snapshot or low `initial_waiting_seconds` | Raise `AIRBYTE_INITIAL_WAITING_SECONDS`, re-apply |
| Airbyte destination: insufficient privileges | LOADER grants missing | Re-run `make snowflake-bootstrap` |
| dbt: source table not found | See F4 | Check case/schema/grants |
| `dbt source freshness` error | Airbyte stopped or schedule off | See `docs/runbooks/failed-sync.md` |
| Dagster: manifest.json does not exist | `dbt deps` not run, or not started via `dagster dev` | `make dbt-deps`; use `make dagster-dev`; in production run `dbt parse` at build time |
| Dagster: env var not set | Not exported | Start through `make` or `set -a; source .env; set +a` |
| Dynamic table creation: change tracking | Raw tables lack change tracking | `make enable-change-tracking` |
| `cdc_health_job` fails: WAL retained | Sync not running or slow | `docs/runbooks/replication-slot-lag.md` |
| `reconciliation_job` drift | Recent data still in flight, deletes lagging, or real loss | Wait one cycle; if persistent, find the diverging keys; consider the full-resync runbook |
| Slot `wal_status = lost` | Hit `max_slot_wal_keep_size` | `docs/runbooks/full-resync-backfill.md` |

---

## 15. Rollback and teardown

**Roll back a bad dbt change:** revert the change, run `make check`, then `make dbt-build DBT_TARGET=prod`; for a bad incremental fact,
`dbt build --full-refresh --select <model>+`. Snowflake Time Travel can restore a dropped table:
`UNDROP TABLE ...` within the retention window.

**Pause the pipeline safely:** disable the Dagster schedules. The slot keeps retaining WAL, so do not pause for long
(see the lag runbook), or drop the slot deliberately and plan a full re-sync.

**Full teardown (dev/demo):**
```bash
$ make down                                      # stops Postgres/generator, deletes volumes (this also removes the slot)
$ abctl local uninstall                          # removes Airbyte
```
```sql
SF> USE ROLE ACCOUNTADMIN;
SF> DROP DATABASE IF EXISTS SHOPSPHERE_ANALYTICS_DEV;
SF> DROP DATABASE IF EXISTS SHOPSPHERE_ANALYTICS;      DROP DATABASE IF EXISTS SHOPSPHERE_RAW;
SF> DROP WAREHOUSE IF EXISTS LOADING_WH; DROP WAREHOUSE IF EXISTS TRANSFORM_WH; DROP WAREHOUSE IF EXISTS BI_WH;
SF> DROP RESOURCE MONITOR IF EXISTS SHOPSPHERE_RM;
SF> DROP USER IF EXISTS SVC_AIRBYTE; DROP USER IF EXISTS SVC_DBT; DROP USER IF EXISTS SVC_BI;
SF> DROP ROLE IF EXISTS PII_READER; DROP ROLE IF EXISTS ANALYST; DROP ROLE IF EXISTS REPORTER;
SF> DROP ROLE IF EXISTS TRANSFORMER; DROP ROLE IF EXISTS LOADER;
```
Never leave an orphaned replication slot on a real source: `SELECT pg_drop_replication_slot('airbyte_slot');`

---

## Appendix A: Command cheat sheet

| Goal | Command |
|---|---|
| Start/stop source | `make up` / `make down` |
| CDC objects | `make cdc-setup` |
| Seed / continuous traffic / smoke test | `make seed` / `make generate` / `make smoke` |
| Snowflake bootstrap | `make snowflake-bootstrap` |
| Airbyte preview / apply | `make airbyte-plan` / `make airbyte-apply` |
| dbt | `make dbt-deps`, `make dbt-build`, `make dbt-build DBT_TARGET=prod`, `make dbt-source-freshness`, `make dbt-docs` |
| Orchestration UI | `make dagster-dev` |
| Dashboards | `make bi` |
| Quality | `make check` (all), `make lint`, `make test` |

## Appendix B: Environment variables by component

| Component | Variables |
|---|---|
| Postgres / generator | `POSTGRES_USER/PASSWORD/DB/HOST/PORT`, `PG_DSN`, `SESSIONS_PER_SEC`, `SEED_CUSTOMERS`, `SEED_PRODUCTS`, `RUN_SECONDS`, `LIFECYCLE_DELAY_SECONDS` |
| CDC role | `AIRBYTE_CDC_PASSWORD` |
| Snowflake bootstrap | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_ADMIN_USER`, `SNOWFLAKE_ADMIN_PASSWORD` or `SNOWFLAKE_ADMIN_PRIVATE_KEY_PATH` (+ passphrase), `SNOWFLAKE_MONTHLY_CREDIT_QUOTA`, `*_RSA_PUBLIC_KEY` |
| dbt | `DBT_TARGET`, `DBT_SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`, `DBT_DEV_SCHEMA_SUFFIX` |
| Airbyte apply | `AIRBYTE_API_URL`, `AIRBYTE_CLIENT_ID/SECRET`, `AIRBYTE_WORKSPACE_ID`, `AIRBYTE_SOURCE_PG_HOST`, `AIRBYTE_SOURCE_PG_SSL_MODE`, `AIRBYTE_INITIAL_WAITING_SECONDS`, `AIRBYTE_SNOWFLAKE_PRIVATE_KEY_PATH` (+ passphrase), `AIRBYTE_SCHEDULE` |
| Dagster | all dbt + Airbyte vars above, `AIRBYTE_CONNECTION_ID`, `PG_DSN`, `SLACK_WEBHOOK_URL`, `CDC_MAX_RETAINED_WAL_MB`, `RECON_TOLERANCE_PCT` |
| BI | `BI_SNOWFLAKE_USER`, `BI_PRIVATE_KEY_PATH`, `BI_PRIVATE_KEY_PASSPHRASE`, `SNOWFLAKE_ACCOUNT` |
