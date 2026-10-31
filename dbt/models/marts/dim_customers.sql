{{ config(
    materialized='table',
    tags=['hot'],
    post_hook=["{{ apply_pii_tags({'email': 'email', 'full_name': 'name'}) }}"]
) }}

select
    customer_id::number(38, 0) as customer_id,
    email::varchar as email,
    full_name::varchar as full_name,
    country::varchar as country,
    created_at::timestamp_tz as created_at,
    updated_at::timestamp_tz as updated_at
from {{ ref('stg_customers') }}
where not is_deleted
