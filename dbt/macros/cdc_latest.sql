{#- Collapse an Airbyte CDC change log (Append mode) to the newest record per primary key.
    Ordering: Postgres LSN first (the true change order), extraction time as tie-breaker.
    Deleted rows are KEPT (flagged via _ab_cdc_deleted_at) so downstream models can soft-delete. -#}
{% macro cdc_latest(source_name, table_name, key_columns) -%}
select *
from {{ source(source_name, table_name) }}
qualify row_number() over (
    partition by {{ key_columns | join(', ') }}
    order by _ab_cdc_lsn desc nulls last, _airbyte_extracted_at desc
) = 1
{%- endmacro %}
