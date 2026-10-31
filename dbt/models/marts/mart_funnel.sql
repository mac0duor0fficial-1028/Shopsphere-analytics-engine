{{ config(tags=['hot']) }}

with agg as (
    select
        session_date,
        count(*) as sessions,
        count_if(reached_view) as viewed_sessions,
        count_if(reached_cart) as cart_sessions,
        count_if(reached_checkout) as checkout_sessions,
        count_if(reached_purchase) as purchase_sessions
    from {{ ref('int_order_funnel') }}
    group by session_date
)

select
    *,
    div0(cart_sessions, viewed_sessions) as view_to_cart_rate,
    div0(checkout_sessions, cart_sessions) as cart_to_checkout_rate,
    div0(purchase_sessions, checkout_sessions) as checkout_to_purchase_rate,
    div0(purchase_sessions, viewed_sessions) as overall_conversion_rate
from agg
