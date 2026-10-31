with latest as (
    {{ cdc_latest('shopsphere_raw', 'events', ['event_id']) }}
)

select
    event_id::number(38, 0) as event_id,
    session_id::varchar as session_id,
    customer_id::number(38, 0) as customer_id,
    lower(trim(event_type)) as event_type,
    product_id::number(38, 0) as product_id,
    order_id::number(38, 0) as order_id,
    occurred_at::timestamp_tz as occurred_at,
    _ab_cdc_deleted_at is not null as is_deleted,
    _airbyte_extracted_at::timestamp_tz as _extracted_at
from latest
