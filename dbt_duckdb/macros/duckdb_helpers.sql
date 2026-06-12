-- macros/duckdb_helpers.sql
-- Reusable macros for the DuckDB build.

{#
  Override the default schema naming so custom schemas are used verbatim
  (e.g. `raw`, `staging`, `marts`) instead of the dbt default of
  `<target_schema>_<custom>`. This keeps the warehouse layout readable and
  lets the source declarations point at a literal `raw` schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}


{# Convert a naive UTC timestamp to WIB wall-clock (Asia/Jakarta, UTC+7).
   timezone('UTC', col)         -> interpret the value as UTC  (TIMESTAMPTZ)
   timezone('Asia/Jakarta', ..) -> render that instant in Jakarta (TIMESTAMP) #}
{% macro to_wib(column) %}
    timezone('Asia/Jakarta', timezone('UTC', {{ column }}))
{% endmacro %}


{# Return the current Jakarta date as a date literal #}
{% macro jakarta_today() %}
    cast(timezone('Asia/Jakarta', now()) as date)
{% endmacro %}


{# Surrogate key from multiple fields using MD5 (works natively in DuckDB) #}
{% macro surrogate_key(field_list) %}
    md5(
        {%- for field in field_list %}
            coalesce(cast({{ field }} as varchar), '')
            {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    )
{% endmacro %}
