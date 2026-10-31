with latest as (
    {{ cdc_latest('shopsphere_raw', 'order_items', ['order_item_id']) }}
)

select
    order_item_id::number(38, 0) as order_item_id,
    order_id::number(38, 0) as order_id,
    product_id::number(38, 0) as product_id,
    quantity::number(38, 0) as quantity,
    unit_price::number(12, 2) as unit_price,
    (quantity * unit_price)::number(12, 2) as line_total,
    created_at::timestamp_tz as created_at,
    _ab_cdc_deleted_at is not null as is_deleted,
    _airbyte_extracted_at::timestamp_tz as _extracted_at
from latest
