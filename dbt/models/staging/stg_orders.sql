with latest as (
    {{ cdc_latest('shopsphere_raw', 'orders', ['order_id']) }}
)

select
    order_id::number(38, 0) as order_id,
    customer_id::number(38, 0) as customer_id,
    lower(trim(status)) as status,
    lower(trim(channel)) as channel,
    total_amount::number(12, 2) as total_amount,
    upper(trim(currency)) as currency,
    created_at::timestamp_tz as created_at,
    updated_at::timestamp_tz as updated_at,
    _ab_cdc_deleted_at is not null as is_deleted,
    _ab_cdc_deleted_at::timestamp_tz as deleted_at,
    coalesce(_ab_cdc_deleted_at::timestamp_tz, updated_at::timestamp_tz) as record_changed_at,
    _airbyte_extracted_at::timestamp_tz as _extracted_at
from latest
