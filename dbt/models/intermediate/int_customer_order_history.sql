{#- Revenue orders per customer in sequence; basis for LTV, repeat rate and cohorts. -#}
with revenue_orders as (
    select
        customer_id,
        order_id,
        created_at as order_ts,
        total_amount
    from {{ ref('int_orders_enriched') }}
    where is_revenue_order and not is_deleted
),

sequenced as (
    select
        customer_id,
        order_id,
        order_ts,
        total_amount,
        row_number() over (partition by customer_id order by order_ts, order_id) as order_sequence,
        lag(order_ts) over (partition by customer_id order by order_ts, order_id) as previous_order_ts
    from revenue_orders
)

select
    customer_id,
    order_id,
    order_ts,
    {{ utc_date('order_ts') }} as order_date,
    date_trunc('month', {{ utc_date('order_ts') }}) as order_month,
    total_amount,
    order_sequence,
    order_sequence = 1 as is_first_order,
    datediff('day', previous_order_ts, order_ts) as days_since_previous_order
from sequenced
