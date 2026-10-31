{{ config(
    enabled=var('enable_realtime', false),
    materialized='dynamic_table',
    snowflake_warehouse=target.warehouse,
    target_lag='5 minutes',
    refresh_mode='AUTO',
    on_configuration_change='apply'
) }}

select
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    i.stock_qty,
    i.updated_at as stock_updated_at,
    case
        when i.stock_qty = 0 then 'out_of_stock'
        when i.stock_qty <= {{ var('low_stock_threshold') }} then 'low_stock'
        else 'in_stock'
    end as stock_status
from {{ ref('stg_inventory') }} as i
inner join {{ ref('stg_products') }} as p on i.product_id = p.product_id
where not i.is_deleted and not p.is_deleted
