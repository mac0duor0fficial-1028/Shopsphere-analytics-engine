{{ config(tags=['hot']) }}

select
    date_trunc('hour', {{ utc_ts('order_ts') }}) as order_hour_utc,
    channel,
    currency,
    count(*) as orders_placed,
    count_if(is_revenue_order) as revenue_orders,
    coalesce(sum(iff(is_revenue_order, total_amount, 0)), 0) as gross_revenue
from {{ ref('fct_orders') }}
where not is_deleted
group by 1, channel, currency
