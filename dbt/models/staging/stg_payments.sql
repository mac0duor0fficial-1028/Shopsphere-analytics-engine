with latest as (
    {{ cdc_latest('shopsphere_raw', 'payments', ['payment_id']) }}
)

select
    payment_id::number(38, 0) as payment_id,
    order_id::number(38, 0) as order_id,
    lower(trim(method)) as payment_method,
    lower(trim(status)) as payment_status,
    amount::number(12, 2) as amount,
    created_at::timestamp_tz as created_at,
    updated_at::timestamp_tz as updated_at,
    _ab_cdc_deleted_at is not null as is_deleted,
    _airbyte_extracted_at::timestamp_tz as _extracted_at
from latest
