with history as (
    select
        customer_id,
        count(*) as order_count,
        sum(total_amount) as lifetime_revenue,
        min(order_ts) as first_order_ts,
        max(order_ts) as last_order_ts,
        avg(days_since_previous_order) as avg_days_between_orders
    from {{ ref('int_customer_order_history') }}
    group by customer_id
),

joined as (
    select
        c.customer_id,
        c.country,
        c.created_at as signup_ts,
        coalesce(h.order_count, 0) as order_count,
        coalesce(h.lifetime_revenue, 0) as lifetime_revenue,
        h.first_order_ts,
        h.last_order_ts,
        h.avg_days_between_orders,
        datediff('day', h.last_order_ts, current_timestamp()) as days_since_last_order
    from {{ ref('dim_customers') }} as c
    left join history as h on c.customer_id = h.customer_id
)

select
    *,
    div0(lifetime_revenue, order_count) as average_order_value,
    order_count > 1 as is_repeat_customer,
    case
        when order_count = 0 then 'prospect'
        when days_since_last_order > {{ var('lapsed_after_days') }} then 'lapsed'
        when order_count > 1 then 'repeat'
        else 'new'
    end as customer_segment
from joined
