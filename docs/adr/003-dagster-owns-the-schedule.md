# ADR 003: Dagster owns scheduling; the Airbyte connection is manual-trigger

**Status:** accepted

**Context.** Two schedulers (Airbyte cron + orchestrator) race: dbt can start before the load finishes.

**Decision.** Airbyte connection schedule is `manual`; Dagster triggers the sync, waits for completion, then builds
dbt models that depend on the Airbyte-produced raw assets. A conflicting running job is attached to, not duplicated.

**Consequences.** Deterministic ordering and one place for retries and alerting. Set `AIRBYTE_SCHEDULE` to a cron
expression to hand scheduling back to Airbyte.
