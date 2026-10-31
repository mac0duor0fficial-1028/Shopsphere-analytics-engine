with sales as (
    select
        product_id,
        sum(quantity) as units_sold,
        sum(line_total) as revenue,
        count(distinct order_id) as orders
    from {{ ref('fct_order_items') }}
    where is_revenue_order and not is_deleted
    group by product_id
),

refunds as (
    select
        product_id,
        sum(quantity) as units_refunded,
        sum(line_total) as refunded_revenue
    from {{ ref('fct_order_items') }}
    where order_status = 'refunded' and not is_deleted
    group by product_id
),

joined as (
    select
        p.product_id,
        p.sku,
        p.product_name,
        p.category,
        p.price,
        p.stock_qty,
        p.stock_status,
        coalesce(s.units_sold, 0) as units_sold,
        coalesce(s.revenue, 0) as revenue,
        coalesce(s.orders, 0) as orders,
        coalesce(r.units_refunded, 0) as units_refunded,
        coalesce(r.refunded_revenue, 0) as refunded_revenue
    from {{ ref('dim_products') }} as p
    left join sales as s on p.product_id = s.product_id
    left join refunds as r on p.product_id = r.product_id
),

ranked as (
    select
        *,
        rank() over (order by revenue desc) as revenue_rank_desc,
        rank() over (order by revenue asc) as revenue_rank_asc
    from joined
)

select
    *,
    revenue_rank_desc <= 10 as is_top_10,
    revenue_rank_asc <= 10 as is_bottom_10,
    div0(units_refunded, units_sold) as refund_rate_units
from ranked
