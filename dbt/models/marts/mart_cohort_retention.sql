with first_orders as (
    select customer_id, order_month as cohort_month
    from {{ ref('int_customer_order_history') }}
    where is_first_order
),

activity as (
    select distinct customer_id, order_month as activity_month
    from {{ ref('int_customer_order_history') }}
),

cohort_sizes as (
    select cohort_month, count(*) as cohort_size
    from first_orders
    group by cohort_month
),

retained as (
    select
        f.cohort_month,
        datediff('month', f.cohort_month, a.activity_month) as months_since_first_order,
        count(distinct a.customer_id) as active_customers
    from first_orders as f
    inner join activity as a on f.customer_id = a.customer_id
    group by f.cohort_month, months_since_first_order
)

select
    r.cohort_month,
    r.months_since_first_order,
    s.cohort_size,
    r.active_customers,
    div0(r.active_customers, s.cohort_size) as retention_rate
from retained as r
inner join cohort_sizes as s on r.cohort_month = s.cohort_month
