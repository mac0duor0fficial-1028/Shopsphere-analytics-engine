{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='payment_id',
    tags=['hot']
) }}

select
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    payment_status = 'captured' as is_captured,
    payment_status = 'refunded' as is_refunded,
    payment_status = 'failed' as is_failed,
    created_at,
    updated_at,
    is_deleted,
    _extracted_at as last_extracted_at
from {{ ref('stg_payments') }}
{% if is_incremental() %}
    where _extracted_at > (
        select dateadd(minute, -{{ var('incremental_lookback_minutes') }}, max(last_extracted_at))
        from {{ this }}
    )
{% endif %}
