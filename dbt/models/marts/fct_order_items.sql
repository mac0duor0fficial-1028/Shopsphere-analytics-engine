{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='order_item_id',
    tags=['hot']
) }}

select
    i.order_item_id,
    i.order_id,
    i.product_id,
    o.customer_id,
    o.status as order_status,
    o.channel,
    o.is_revenue_order,
    i.quantity,
    i.unit_price,
    i.line_total,
    o.created_at as order_ts,
    {{ utc_date('o.created_at') }} as order_date,
    (i.is_deleted or o.is_deleted) as is_deleted,
    greatest(i._extracted_at, o.order_last_extracted_at) as last_extracted_at
from {{ ref('stg_order_items') }} as i
inner join {{ ref('int_orders_enriched') }} as o on i.order_id = o.order_id
{% if is_incremental() %}
    where greatest(i._extracted_at, o.order_last_extracted_at) > (
        select dateadd(minute, -{{ var('incremental_lookback_minutes') }}, max(last_extracted_at))
        from {{ this }}
    )
{% endif %}
