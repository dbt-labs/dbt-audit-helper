{% macro compare_relations(a_relation, b_relation, exclude_columns=[], primary_key=None, summarize=true) %}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=a_relation, except=exclude_columns) %}

{% set column_selection %}

  {% for column_name in column_names %} 
    {{ adapter.quote(column_name) }} 
    {% if not loop.last %}
      , 
    {% endif %} 
  {% endfor %}

{% endset %}

{% set a_query %}
select

  {{ column_selection }}

from {{ a_relation }}
{% endset %}

{% set b_query %}
select

  {{ column_selection }}

from {{ b_relation }}
{% endset %}

{{ audit_helper.compare_queries(a_query, b_query, primary_key, summarize) }}

{% endmacro %}
