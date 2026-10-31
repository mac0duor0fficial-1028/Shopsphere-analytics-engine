{#- One row per browsing session with the first timestamp reached at each funnel stage. -#}
select
    session_id,
    min(occurred_at) as session_start_at,
    {{ utc_date('min(occurred_at)') }} as session_date,
    max(customer_id) as customer_id,
    min(case when event_type = 'product_view' then occurred_at end) as viewed_at,
    min(case when event_type = 'add_to_cart' then occurred_at end) as carted_at,
    min(case when event_type = 'begin_checkout' then occurred_at end) as checkout_at,
    min(case when event_type = 'purchase' then occurred_at end) as purchased_at,
    max(case when event_type = 'purchase' then order_id end) as order_id,
    count_if(event_type = 'product_view') > 0 as reached_view,
    count_if(event_type = 'add_to_cart') > 0 as reached_cart,
    count_if(event_type = 'begin_checkout') > 0 as reached_checkout,
    count_if(event_type = 'purchase') > 0 as reached_purchase
from {{ ref('stg_events') }}
where not is_deleted
group by session_id
