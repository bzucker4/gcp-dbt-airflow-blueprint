{#
    Dataset routing (public preview).

    prod   : models land in their configured custom schema (staging, warehouse).
    others : every model lands in the developer's own dbt_<user>_scratch dataset,
             so dev and CI can never write into production datasets.

    The full macro ships with the complete blueprint: https://payhip.com/b/YIq4E
    Until then, this preview falls back to dbt's default behavior.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ default__generate_schema_name(custom_schema_name, node) }}
{%- endmacro %}
