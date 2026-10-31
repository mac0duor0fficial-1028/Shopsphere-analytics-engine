{{ config(severity='warn') }}
-- Order header total must equal the sum of its lines (can lag by one sync => warn, not error).
select order_id, total_amount, items_total
from {{ ref('fct_orders') }}
where not is_deleted and not is_total_consistent
