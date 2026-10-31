{#- Near-real-time revenue straight off the deduped CDC log (bypasses dbt's batch cadence).
    End-to-end freshness = Airbyte sync interval + target_lag. Disabled unless enable_realtime=true. -#}
{{ config(
    enabled=var('enable_realtime', false),
    materialized='dynamic_table',
    snowflake_warehouse=target.warehouse,
    target_lag='5 minutes',
    refresh_mode='AUTO',
    on_configuration_change='apply'
) }}

select
    date_trunc('minute', created_at) as minute_ts,
    count(*) as orders,
    sum(total_amount) as gross_revenue
from {{ ref('stg_orders') }}
where not is_deleted and status in ({{ revenue_statuses_sql() }})
group by 1
