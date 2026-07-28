{#
  dbt's default generate_schema_name concatenates <profile_schema>_<custom_schema>
  (e.g. RAW_STAGING). We want models/staging/* and models/core/* to land in
  exactly STAGING and CORE, matching snowflake/setup.sql's schema names --
  this is the standard dbt-labs-documented override for that case.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
