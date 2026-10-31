# Runbook: source schema drift

Connection policy: non-breaking changes (new columns) propagate; breaking changes pause the connection.

**New column in Postgres**
1. Add it to the publication if needed (publications on named tables include new columns automatically).
2. Airbyte propagates the column to `RAW.AIRBYTE.<table>` on the next sync.
3. Add it to the matching `stg_*` model and downstream marts. `fct_orders` has `on_schema_change: fail`
   and an enforced contract: update the contract in `_marts__models.yml`, then `dbt build --full-refresh -s fct_orders`.

**Column dropped / type changed (breaking)**
1. The connection pauses. Assess impact with `dbt ls --select +<model>` and lineage in dbt docs.
2. Update models first, then resume the connection and refresh the schema in Airbyte.
3. For type changes Airbyte may need a refresh of the affected stream (see full-resync runbook).

**New table**: create it, add to the publication (`ALTER PUBLICATION shopsphere_pub ADD TABLE ...`), add the
stream in `airbyte/apply_airbyte.py` (`TABLES`), add a source entry and `stg_` model.
