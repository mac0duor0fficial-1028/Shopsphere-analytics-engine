# Runbook: failed or stuck Airbyte sync

1. Dagster `hot_pipeline_job` failed at `airbyte_cdc_sync`: open the Airbyte job (the job id is in the run log).
2. Common causes and fixes:
   - **Postgres unreachable / auth**: check `airbyte_cdc` password, network, SSL mode.
   - **Snowflake auth / suspended warehouse / resource monitor**: confirm `SVC_AIRBYTE` key, `LOADING_WH` state, credit quota.
   - **Initial-snapshot timeout**: raise `AIRBYTE_INITIAL_WAITING_SECONDS` and re-apply (`make airbyte-apply`).
   - **Schema change paused the connection**: see [schema-drift.md](schema-drift.md).
3. Because the source uses `lsn_commit_behaviour = After loading Data in the destination`, a failed sync
   does not advance the slot. Re-running is safe; no data is lost, and rows may be re-delivered (dedup handles it).
4. After recovery verify: `dbt source freshness` passes and the next `reconciliation_job` is green.
