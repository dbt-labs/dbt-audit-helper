{% macro compare_all_columns( a_relation, b_relation, primary_key,  exclude_columns=[],summarize=true ) -%}
  {{ return(adapter.dispatch('compare_all_columns', 'audit_helper')( a_relation, b_relation, primary_key, exclude_columns, summarize )) }}
{%- endmacro %}

{% macro default__compare_all_columns( a_relation, b_relation, primary_key, exclude_columns=[], summarize=true ) -%}

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

  {% for column_name in column_names %}

    {% set audit_query = audit_helper.compare_column_values_verbose(
      a_query=a_query,
      b_query=b_query,
      primary_key=primary_key,
      column_to_compare=column_name
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

    ), 
    
      {%- if summarize %}

        final as (
          select
            upper(column_name) as column_name,
            sum(case when perfect_match then 1 else 0 end) as perfect_match,
            sum(case when null_in_a then 1 else 0 end) as null_in_a,
            sum(case when null_in_b then 1 else 0 end) as null_in_b,
            sum(case when missing_from_a then 1 else 0 end) as missing_from_a,
            sum(case when missing_from_b then 1 else 0 end) as missing_from_b,
            sum(case when conflicting_values then 1 else 0 end) as conflicting_values
          from main
          group by 1
          order by column_name
        )

      {%- else %}

        final as (
          select
            primary_key,           
            upper(column_name) as column_name,
            perfect_match,
            null_in_a,
            null_in_b,
            missing_from_a,
            missing_from_b,
            conflicting_values
          from main    
          order by primary_key
        )

      {%- endif %}

      select * from final
    
    {% endif %}

  {% endfor %}
    
{% endmacro %}