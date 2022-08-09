{% macro compare_relations(a_relation, b_relation, exclude_columns=[], primary_key=None) %}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=a_relation, except=exclude_columns) %}

{% set check_cols_csv = '"%s"' %'", "'.join(column_names) %}
-- note: I tried to use this less hacky approach ( https://stackoverflow.com/a/12007707/5037635 ),
-- but Jinja doesn't seem to allow it, even though it should work in Python.

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
