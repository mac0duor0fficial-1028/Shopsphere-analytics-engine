SHELL := /bin/bash
.DEFAULT_GOAL := help
-include .env
export

PY      ?= python3
DBT     := cd dbt && dbt
COMPOSE := docker compose

.PHONY: help
help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-24s %s\n", $$1, $$2}'

# ----------------------------------------------------------------- source system
.PHONY: up down cdc-setup seed generate smoke
up: ## Start local Postgres (logical replication enabled)
	$(COMPOSE) up -d --wait postgres
down: ## Stop everything and delete volumes
	$(COMPOSE) --profile generator down -v
cdc-setup: ## Create CDC role, publication and replication slot
	$(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 \
	  -v airbyte_password="$(AIRBYTE_CDC_PASSWORD)" < source_db/ddl/02_cdc.sql
seed: ## Run generator for 30s to seed reference data and first orders
	RUN_SECONDS=30 $(COMPOSE) --profile generator run --rm generator
smoke: ## Source smoke test (needs `make up`): re-apply DDL, run generator 20s, assert orders exist
	$(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -v ON_ERROR_STOP=1 -q < source_db/ddl/01_schema.sql
	RUN_SECONDS=20 SESSIONS_PER_SEC=20 LIFECYCLE_DELAY_SECONDS=1 $(COMPOSE) --profile generator run --rm generator
	$(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -tAc "select count(*) from shop.orders" | grep -Eq '^[1-9]'
	@echo "Source smoke test passed."
generate: ## Run the traffic generator continuously (Ctrl-C to stop)
	$(COMPOSE) --profile generator up generator

# ----------------------------------------------------------------- snowflake
.PHONY: snowflake-bootstrap enable-change-tracking
snowflake-bootstrap: ## Create warehouses, roles, DBs, schemas, users, masking (needs admin creds)
	$(PY) scripts/snowflake_sql.py --role ACCOUNTADMIN infra/snowflake/sql/01_account_objects.sql \
	  infra/snowflake/sql/02_grants.sql infra/snowflake/sql/03_service_users.sql infra/snowflake/sql/04_governance.sql
enable-change-tracking: ## Needed for Dynamic Tables; re-run if Airbyte recreates raw tables
	$(PY) scripts/snowflake_sql.py --role LOADER infra/snowflake/sql/90_enable_change_tracking.sql

# ----------------------------------------------------------------- airbyte
.PHONY: airbyte-apply airbyte-plan
airbyte-plan: ## Print the Airbyte API payloads without applying
	$(PY) airbyte/apply_airbyte.py --dry-run
airbyte-apply: ## Create/update Airbyte source, destination and connection (idempotent)
	$(PY) airbyte/apply_airbyte.py

# ----------------------------------------------------------------- dbt
.PHONY: dbt-deps dbt-build dbt-source-freshness dbt-docs dbt-parse
dbt-deps: ## Install dbt packages
	$(DBT) deps
dbt-parse: ## Parse project (no warehouse connection needed)
	$(DBT) parse
dbt-source-freshness: ## Check source freshness
	$(DBT) source freshness
dbt-build: ## Run models + snapshots + tests
	$(DBT) build
dbt-docs: ## Generate docs
	$(DBT) docs generate

# ----------------------------------------------------------------- orchestration / bi
.PHONY: dagster-dev bi
dagster-dev: ## Start Dagster UI (http://localhost:3000)
	mkdir -p orchestration/.dagster_home
	cd orchestration && DAGSTER_HOME=$$PWD/.dagster_home dagster dev
bi: ## Start Streamlit dashboards
	streamlit run bi/streamlit_app/app.py

# ----------------------------------------------------------------- quality
.PHONY: lint test fmt check
lint: ## Lint SQL, YAML and Python
	sqlfluff lint dbt/models dbt/snapshots dbt/tests
	yamllint .
	black --check --line-length 100 source_db scripts airbyte orchestration bi
test: ## Python unit tests (definitions test needs `make dbt-deps` first; skipped otherwise)
	pytest -q source_db/generator/tests airbyte/tests orchestration/tests
fmt: ## Auto-format
	black --line-length 100 source_db scripts airbyte orchestration bi
	sqlfluff fix dbt/models dbt/snapshots dbt/tests
check: dbt-deps lint dbt-parse test ## Full local quality gate (run before every prod build)
	@echo "All checks passed."
