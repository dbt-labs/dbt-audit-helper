{% macro compare_all_columns(model_name, primary_key, prod_schema, exclude_columns, updated_at_column) -%}
  {{ return(adapter.dispatch('compare_all_columns', 'audit_helper')( model_name, primary_key, prod_schema, exclude_columns, updated_at_column )) }}
{%- endmacro %}

{% macro default__compare_all_columns( model_name, primary_key, prod_schema, exclude_columns, updated_at_column ) -%}


  {% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref(model_name), except=exclude_columns) %}

  {% set old_etl_relation_query %}
      select * from {{prod_schema}}.{{ model_name }}
  {% endset %}

  {% set new_etl_relation_query %}
    select * from {{ ref(model_name) }}
  {% endset %}

  {% for column in column_names %}

    {% set audit_query = audit_helper.compare_column_values_count(
      a_query=old_etl_relation_query,
      b_query=new_etl_relation_query,
      primary_key=primary_key,
      column_to_compare=column,
      updated_at_column='{{updated_at_column}}'
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

    {% endif %}

  {% endfor %}
    
{% endmacro %}