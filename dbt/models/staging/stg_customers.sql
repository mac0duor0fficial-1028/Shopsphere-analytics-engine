{{ config(post_hook=["{{ apply_pii_tags({'email': 'email', 'full_name': 'name'}) }}"]) }}

with latest as (
    {{ cdc_latest('shopsphere_raw', 'customers', ['customer_id']) }}
)

select
    customer_id::number(38, 0) as customer_id,
    lower(trim(email)) as email,
    trim(full_name) as full_name,
    upper(country) as country,
    created_at::timestamp_tz as created_at,
    updated_at::timestamp_tz as updated_at,
    _ab_cdc_deleted_at is not null as is_deleted,
    _ab_cdc_deleted_at::timestamp_tz as deleted_at,
    coalesce(_ab_cdc_deleted_at::timestamp_tz, updated_at::timestamp_tz) as record_changed_at,
    _airbyte_extracted_at::timestamp_tz as _extracted_at
from latest
