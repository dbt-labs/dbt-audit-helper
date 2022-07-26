{% macro pop_columns(model_name, columns_to_pop) %}
{%- set all_columns = adapter.get_columns_in_relation(model_name) -%}
{% set popped_columns=[] %}

{% for column in all_columns %}
    {% if column.name | lower not in columns_to_pop | lower %}
        {% do popped_columns.append(column) %}
    {% endif %}
{% endfor %}

{{ return(popped_columns) }}

{% endmacro %}


{% macro compare_all_columns(model_name, primary_key, prod_schema, exclude_columns, updated_at_column, exclude_recent_hours, direct_conflicts_only) -%}
  {{ return(adapter.dispatch('compare_all_columns', 'audit_helper')( model_name, primary_key, prod_schema, exclude_columns, updated_at_column, exclude_recent_hours, direct_conflicts_only )) }}
{%- endmacro %}

{% macro default__compare_all_columns( model_name, primary_key, prod_schema, exclude_columns, updated_at_column, exclude_recent_hours, direct_conflicts_only ) -%}


  {% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref(model_name), except=exclude_columns) %}

  {% set old_etl_relation_query %}
      select * from {{prod_schema}}.{{ model_name }}
      where {{updated_at_column}} < dateadd(hour, -{{exclude_recent_hours}}, current_timestamp)
  {% endset %}

  {% set new_etl_relation_query %}
    select * from {{ ref(model_name) }}
    where {{updated_at_column}} < dateadd(hour, -{{exclude_recent_hours}}, current_timestamp)
  {% endset %}

  {% for column in column_names %}

    {% set audit_query = audit_helper.compare_column_values(
      a_query=old_etl_relation_query,
      b_query=new_etl_relation_query,
      primary_key=primary_key,
      column_to_compare=column
    ) %}

    /*  Create a query combining results from all columns so that the user, or the 
    test suite, can examine all at once.
    */
    {% if loop.first %}
    /*  Create a CTE that wraps all the unioned subqueries that are created
        in this for loop
    */
      with main as ( 
    {% endif %}
    /*  There will be one audit_query subquery for each column
    */
    ( {{ audit_query }} )
    {% if not loop.last %}
      union
    {% else %}
    ) select * from main 
    /*  Identify records that are not perfect matches. These are dbt test failures.
        - if direct_conflicts_only = true, ONLY a direct conflict will raise an error.
        - if direct_conflicts_only !- true, anything other than '\u2705: perfect match' will raise an error.
    */
      {% if direct_conflicts_only is true %}
        where match_status = '\u1F645: ‍values do not match' and count_records > 0 
      {% else %}
        where match_status != '\u2705: perfect match' and count_records > 0 
      {% endif %}
    

    {% endif %}

  {% endfor %}
    
{% endmacro %}