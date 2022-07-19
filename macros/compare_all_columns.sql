{% macro compare_all_columns(model_name, primary_key, prod_schema, exclude_columns ) -%}
  {{ return(adapter.dispatch('compare_all_columns', 'audit_helper')( model_name, primary_key, prod_schema, exclude_columns )) }}
{%- endmacro %}

{% macro default__compare_all_columns( model_name, primary_key, prod_schema, exclude_columns ) -%}

  {% set columns_to_compare=audit_helper.pop_columns(model_name, exclude_columns) %}

  {% set old_etl_relation_query %}
      select * from {{prod_schema}}.{{ model_name }}
  {% endset %}

  {% set new_etl_relation_query %}
    select * from {{ ref(model_name) }}
  {% endset %}

  {% for column in columns_to_compare %}

    {% set audit_query = audit_helper.compare_column_values(
      a_query=old_etl_relation_query,
      b_query=new_etl_relation_query,
      primary_key=primary_key,
      column_to_compare=column.name
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
    */
    where match_status != '\u2705: perfect match' and count_records > 0 

    {% endif %}

  {% endfor %}
    
{% endmacro %}