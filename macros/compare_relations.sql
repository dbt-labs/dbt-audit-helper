{% macro compare_relations(a_relation, b_relation, exclude_columns=[], primary_key=None) %}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=a_relation, except=exclude_columns) %}

{% if target.name == 'bigquery' %}
  {% set check_cols_csv = '`%s`' %'`, `'.join(column_names) %}
  -- bigquery likes it like this: select `col_a`, `col_b` from 
{% else %}
  {% set check_cols_csv = '"%s"' %'", "'.join(column_names) %}
  -- everyone else likes it like this: select "col_a", "col_b" from
{% endif %}

{% set a_query %}
select
    {{ check_cols_csv }}

from {{ a_relation }}
{% endset %}

{% set b_query %}
select
    {{ check_cols_csv }}

from {{ b_relation }}
{% endset %}

{{ audit_helper.compare_queries(a_query, b_query, primary_key) }}

{% endmacro %}
