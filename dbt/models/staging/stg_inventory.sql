with latest as (
    {{ cdc_latest('shopsphere_raw', 'inventory', ['product_id']) }}
)

select
    product_id::number(38, 0) as product_id,
    stock_qty::number(38, 0) as stock_qty,
    updated_at::timestamp_tz as updated_at,
    _ab_cdc_deleted_at is not null as is_deleted,
    _airbyte_extracted_at::timestamp_tz as _extracted_at
from latest
