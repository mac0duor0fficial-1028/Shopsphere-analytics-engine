# Runbook: replication slot lag / WAL growth

**Alert:** `cdc_health_job` fails ("slot retains N MB of WAL" or `wal_status` = `unreserved`/`lost`).

**Note:** `active = false` between syncs is normal (Airbyte connects only while syncing). Do not alert on it.

1. Inspect the slot:
   ```sql
   SELECT slot_name, active, wal_status,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal,
          pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS unconfirmed
   FROM pg_replication_slots WHERE slot_name = 'airbyte_slot';
   ```
2. Check why Airbyte is not consuming: Dagster run history (`hot_pipeline_job`), Airbyte job logs,
   Snowflake credit/resource-monitor suspension (`SHOW RESOURCE MONITORS`).
3. Fix the cause and re-run the sync. Retained WAL drops after Airbyte confirms the next LSN.
4. Low-traffic databases: a slot on a quiet table can stop advancing while the rest of the instance
   generates WAL. Keep a heartbeat enabled on the Airbyte source or write a periodic row to a heartbeat table.
5. If `wal_status = 'lost'` (hit `max_slot_wal_keep_size`): the slot is invalid. Follow
   [full-resync-backfill.md](full-resync-backfill.md).
6. **Emergency only** (disk about to fill and Airbyte cannot recover): `SELECT pg_drop_replication_slot('airbyte_slot');`
   then run the full re-sync runbook. This discards unread changes.
