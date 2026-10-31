{{ config(severity='warn') }}
select p.payment_id, p.order_id, p.amount, o.total_amount
from {{ ref('fct_payments') }} as p
inner join {{ ref('fct_orders') }} as o on p.order_id = o.order_id
where not p.is_deleted and not o.is_deleted and p.amount != o.total_amount
