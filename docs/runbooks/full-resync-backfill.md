# Runbook: full re-sync / backfill

Use when the slot is lost, a table was corrupted, or history must be rebuilt.

1. Pause Dagster schedules (`hot_pipeline_job`, `daily_full_job`).
2. If the slot was lost/dropped, recreate it: `make cdc-setup` (idempotent).
3. In Airbyte, **Refresh** the affected streams ("Refresh and retain history" keeps prior change-log rows so
   dbt snapshots and history stay intact; "Refresh and clear" starts clean).
4. Wait for the initial snapshot to finish (initial waiting time may need raising for large tables).
5. Rebuild dependent models:
   `dbt build --full-refresh --select +fct_orders+ +fct_order_items+ +fct_payments+` (add `--exclude tag:realtime` if disabled).
6. Re-enable change tracking if tables were recreated: `make enable-change-tracking`.
7. Run `reconciliation_job`; only resume schedules when drift is within tolerance.
