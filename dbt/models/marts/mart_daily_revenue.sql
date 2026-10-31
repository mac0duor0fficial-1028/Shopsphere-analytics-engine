{{ config(tags=['hot']) }}

with agg as (
    select
        order_date,
        channel,
        currency,
        count(*) as orders_placed,
        count_if(is_revenue_order) as revenue_orders,
        coalesce(sum(iff(is_revenue_order, total_amount, 0)), 0) as gross_revenue,
        count_if(is_cancelled) as cancelled_orders,
        count_if(is_refunded) as refunded_orders,
        coalesce(sum(iff(is_refunded, total_amount, 0)), 0) as refunded_amount,
        coalesce(sum(iff(is_revenue_order, unit_count, 0)), 0) as units_sold
    from {{ ref('fct_orders') }}
    where not is_deleted
    group by order_date, channel, currency
)

select
    *,
    div0(gross_revenue, revenue_orders) as average_order_value,
    div0(cancelled_orders, orders_placed) as cancellation_rate,
    div0(refunded_orders, orders_placed) as refund_rate
from agg
