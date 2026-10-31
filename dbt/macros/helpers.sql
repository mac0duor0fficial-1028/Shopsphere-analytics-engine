{% macro revenue_statuses_sql() -%}
    {%- for s in var('revenue_statuses') -%}'{{ s }}'{{ ', ' if not loop.last }}{%- endfor -%}
{%- endmacro %}

{#- All reporting is in UTC so numbers do not depend on a session time zone. -#}
{% macro utc_date(column) -%}
    convert_timezone('UTC', {{ column }})::date
{%- endmacro %}

{% macro utc_ts(column) -%}
    convert_timezone('UTC', {{ column }})::timestamp_ntz
{%- endmacro %}

{#- Tag PII columns so the account-level masking policy applies (see infra/snowflake/sql/04_governance.sql).
    Tags are lost whenever a table is recreated, so this runs as a post-hook after every build. -#}
{% macro apply_pii_tags(columns) -%}
    {%- if execute and target.name in ['prod', 'dev'] -%}
        {%- set kind = 'view' if config.get('materialized') == 'view' else 'table' -%}
        {%- for col, tag_value in columns.items() %}
            alter {{ kind }} {{ this }} modify column {{ col }}
                set tag shopsphere_analytics.governance.pii = '{{ tag_value }}';
        {%- endfor %}
    {%- endif -%}
{%- endmacro %}
