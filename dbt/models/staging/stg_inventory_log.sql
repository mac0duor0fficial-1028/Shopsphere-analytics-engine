{#- Every historical stock level (not deduped to latest) - feeds fct_inventory_daily.
    One row per (product, source updated_at); replayed CDC events collapse to one. -#}
select
    product_id::number(38, 0) as product_id,
    stock_qty::number(38, 0) as stock_qty,
    updated_at::timestamp_tz as changed_at,
    {{ utc_ts('updated_at') }} as changed_at_utc
from {{ source('shopsphere_raw', 'inventory') }}
where _ab_cdc_deleted_at is null
qualify row_number() over (
    partition by product_id, updated_at
    order by _ab_cdc_lsn desc nulls last, _airbyte_extracted_at desc
) = 1
