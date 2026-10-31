{{ config(tags=['hot']) }}

select
    p.product_id,
    p.sku,
    p.product_name,
    p.category,
    p.price,
    p.is_active,
    coalesce(i.stock_qty, 0) as stock_qty,
    case
        when coalesce(i.stock_qty, 0) = 0 then 'out_of_stock'
        when i.stock_qty <= {{ var('low_stock_threshold') }} then 'low_stock'
        else 'in_stock'
    end as stock_status,
    p.created_at,
    p.updated_at
from {{ ref('stg_products') }} as p
left join {{ ref('stg_inventory') }} as i
    on p.product_id = i.product_id and not i.is_deleted
where not p.is_deleted
