{% macro compare_all_columns( a_relation, b_relation, primary_key, exclude_columns ) -%}
  {{ return(adapter.dispatch('compare_all_columns', 'audit_helper')( a_relation, b_relation, primary_key, exclude_columns )) }}
{%- endmacro %}

{% macro default__compare_all_columns( a_relation, b_relation, primary_key, exclude_columns ) -%}

  {% set column_names = dbt_utils.get_filtered_columns_in_relation(from=a_relation, except=exclude_columns) %}

  {% set a_query %}      
    select
      *
    from {{ a_relation }}
  {% endset %}

  {% set b_query %}
    select
      *
    from {{ b_relation }}
  {% endset %}

  {% for column in column_names %}

    {% set audit_query = audit_helper.compare_column_values_verbose(
      a_query=a_query,
      b_query=b_query,
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
      union all
    {% else %}
    ) select * from main     

    {% endif %}

  {% endfor %}
    
{% endmacro %}