{#- One row per order with item and payment rollups and business flags.
    Deleted orders are kept (is_deleted) so incremental facts can soft-delete them. -#}
with orders as (
    select * from {{ ref('stg_orders') }}
),

items as (
    select
        order_id,
        count(*) as line_count,
        sum(quantity) as unit_count,
        sum(line_total) as items_total,
        max(_extracted_at) as items_extracted_at
    from {{ ref('stg_order_items') }}
    where not is_deleted
    group by order_id
),

latest_payment as (
    select
        order_id,
        payment_method,
        payment_status,
        _extracted_at as payment_extracted_at
    from {{ ref('stg_payments') }}
    where not is_deleted
    qualify row_number() over (partition by order_id order by created_at desc, payment_id desc) = 1
)

select
    o.order_id,
    o.customer_id,
    o.status,
    o.channel,
    o.currency,
    o.total_amount,
    o.created_at,
    o.updated_at,
    o.is_deleted,
    coalesce(i.line_count, 0) as line_count,
    coalesce(i.unit_count, 0) as unit_count,
    coalesce(i.items_total, 0)::number(12, 2) as items_total,
    p.payment_method,
    p.payment_status,
    o.status in ({{ revenue_statuses_sql() }}) as is_revenue_order,
    o.status = 'cancelled' as is_cancelled,
    (o.status = 'refunded' or p.payment_status = 'refunded') and o.status != 'cancelled' as is_refunded,
    abs(o.total_amount - coalesce(i.items_total, 0)) < 0.005 as is_total_consistent,
    greatest(
        o._extracted_at,
        coalesce(i.items_extracted_at, o._extracted_at),
        coalesce(p.payment_extracted_at, o._extracted_at)
    ) as order_last_extracted_at
from orders as o
left join items as i on o.order_id = i.order_id
left join latest_payment as p on o.order_id = p.order_id
