{% macro compare_relation_columns(a_relation, b_relation) %}
  {{ return(adapter.dispatch('compare_relation_columns', 'audit_helper')(a_relation, b_relation)) }}
{% endmacro %}

{% macro default__compare_relation_columns(a_relation, b_relation) %}

with a_cols as (
    {{ audit_helper.get_columns_in_relation_sql(a_relation) }}
),

b_cols as (
    {{ audit_helper.get_columns_in_relation_sql(b_relation) }}
)

select
    column_name,
    a_cols.ordinal_position as a_ordinal_position,
    b_cols.ordinal_position as b_ordinal_position,
    a_cols.data_type as a_data_type,
    b_cols.data_type as b_data_type,
    coalesce(a_cols.ordinal_position = b_cols.ordinal_position, false) as has_ordinal_position_match,
    coalesce(a_cols.data_type = b_cols.data_type, false) as has_data_type_match
from a_cols
full outer join b_cols using (column_name)
order by coalesce(a_cols.ordinal_position, b_cols.ordinal_position)

{% endmacro %}


{% macro get_columns_in_relation_sql(relation) %}

{{ adapter.dispatch('get_columns_in_relation_sql', 'audit_helper')(relation) }}

{% endmacro %}

{% macro default__get_columns_in_relation_sql(relation) %}
    
  {% set columns = adapter.get_columns_in_relation(relation) %}
  {% for column in columns %}
    select 
      {{ dbt.string_literal(column.name) }} as column_name, 
      {{ loop.index }} as ordinal_position,
      {{ dbt.string_literal(column.data_type) }} as data_type

  {% if not loop.last -%}
    union all 
  {%- endif %}
  {% endfor %}


{% endmacro %}

{% macro redshift__get_columns_in_relation_sql(relation) %}
  {# You can't store the results of an info schema query to a table/view in Redshift, because the data only lives on the leader node #}
  {{ return (audit_helper.default__get_columns_in_relation_sql(relation)) }}
{% endmacro %}


{% macro snowflake__get_columns_in_relation_sql(relation) %}
{#-
From: https://github.com/dbt-labs/dbt/blob/dev/louisa-may-alcott/plugins/snowflake/dbt/include/snowflake/macros/adapters.sql#L48
Edited to include ordinal_position
-#}
  select
      ordinal_position,
      column_name,
      data_type,
      character_maximum_length,
      numeric_precision,
      numeric_scale

  from
  {{ relation.information_schema('columns') }}

  where table_name ilike '{{ relation.identifier }}'
    {% if relation.schema %}
    and table_schema ilike '{{ relation.schema }}'
    {% endif %}
    {% if relation.database %}
    and table_catalog ilike '{{ relation.database }}'
    {% endif %}
  order by ordinal_position
{% endmacro %}


{% macro postgres__get_columns_in_relation_sql(relation) %}
{#-
From: https://github.com/dbt-labs/dbt/blob/23484b18b71010f701b5312f920f04529ceaa6b2/plugins/postgres/dbt/include/postgres/macros/adapters.sql#L32
Edited to include ordinal_position
-#}
  select
      ordinal_position,
      column_name,
      data_type,
      character_maximum_length,
      numeric_precision,
      numeric_scale

  from {{ relation.information_schema('columns') }}
  where table_name = '{{ relation.identifier }}'
    {% if relation.schema %}
    and table_schema = '{{ relation.schema }}'
    {% endif %}
  order by ordinal_position
{% endmacro %}


{% macro bigquery__get_columns_in_relation_sql(relation) %}

  select
      ordinal_position,
      column_name,
      data_type

  from `{{ relation.database }}`.`{{ relation.schema }}`.INFORMATION_SCHEMA.COLUMNS
  where table_name = '{{ relation.identifier }}'

{% endmacro %}
