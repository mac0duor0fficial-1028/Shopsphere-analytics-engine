{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_id',
    on_schema_change='fail',
    tags=['hot']
) }}

select
    order_id::number(38, 0) as order_id,
    customer_id::number(38, 0) as customer_id,
    status::varchar as status,
    channel::varchar as channel,
    currency::varchar as currency,
    total_amount::number(12, 2) as total_amount,
    line_count::number(38, 0) as line_count,
    unit_count::number(38, 0) as unit_count,
    items_total::number(12, 2) as items_total,
    payment_method::varchar as payment_method,
    payment_status::varchar as payment_status,
    is_revenue_order::boolean as is_revenue_order,
    is_cancelled::boolean as is_cancelled,
    is_refunded::boolean as is_refunded,
    is_total_consistent::boolean as is_total_consistent,
    is_deleted::boolean as is_deleted,
    created_at::timestamp_tz as order_ts,
    {{ utc_date("created_at") }} as order_date,
    updated_at::timestamp_tz as updated_at,
    order_last_extracted_at::timestamp_tz as order_last_extracted_at
from {{ ref('int_orders_enriched') }}
{% if is_incremental() %}
    where order_last_extracted_at > (
        select dateadd(minute, -{{ var('incremental_lookback_minutes') }}, max(order_last_extracted_at))
        from {{ this }}
    )
{% endif %}
