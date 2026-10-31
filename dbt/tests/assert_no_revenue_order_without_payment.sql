{{ config(severity='warn') }}
-- A paid/shipped/delivered order must have a captured payment.
select order_id, status, payment_status
from {{ ref('fct_orders') }}
where not is_deleted and is_revenue_order and coalesce(payment_status, 'none') != 'captured'
