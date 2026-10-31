with latest as (
    {{ cdc_latest('shopsphere_raw', 'products', ['product_id']) }}
)

select
    product_id::number(38, 0) as product_id,
    sku,
    trim(name) as product_name,
    trim(category) as category,
    price::number(12, 2) as price,
    is_active::boolean as is_active,
    created_at::timestamp_tz as created_at,
    updated_at::timestamp_tz as updated_at,
    _ab_cdc_deleted_at is not null as is_deleted,
    _ab_cdc_deleted_at::timestamp_tz as deleted_at,
    coalesce(_ab_cdc_deleted_at::timestamp_tz, updated_at::timestamp_tz) as record_changed_at,
    _airbyte_extracted_at::timestamp_tz as _extracted_at
from latest
