{#- Closing stock per product per day, carried forward on days with no change. -#}
with changes as (
    select product_id, stock_qty, changed_at, changed_at_utc
    from {{ ref('stg_inventory_log') }}
),

bounds as (
    select min(changed_at_utc)::date as start_date from changes
),

days as (
    select d.date_day
    from {{ ref('dim_date') }} as d
    cross join bounds as b
    where d.date_day between b.start_date and current_date()
),

products as (
    select distinct product_id from changes
),

grid as (
    select
        d.date_day,
        p.product_id
    from days as d
    cross join products as p
),

closing as (
    select
        g.date_day,
        g.product_id,
        c.stock_qty,
        c.changed_at
    from grid as g
    left join changes as c
        on
            g.product_id = c.product_id
            and c.changed_at_utc < dateadd(day, 1, g.date_day)::timestamp_ntz
    qualify row_number() over (partition by g.date_day, g.product_id order by c.changed_at desc) = 1
)

select
    date_day,
    product_id,
    stock_qty as closing_stock_qty,
    stock_qty = 0 as is_stocked_out,
    stock_qty <= {{ var('low_stock_threshold') }} as is_low_stock,
    changed_at as last_change_at
from closing
where stock_qty is not null
