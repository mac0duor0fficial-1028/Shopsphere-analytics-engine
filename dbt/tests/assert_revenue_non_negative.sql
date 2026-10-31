select order_date, channel, gross_revenue
from {{ ref('mart_daily_revenue') }}
where gross_revenue < 0 or refunded_amount < 0
