{% macro compare_which_columns_differ(a_relation, b_relation, primary_key, exclude_columns=[]) %}
    {{ return(adapter.dispatch('compare_which_columns_differ', 'audit_helper')(a_relation, b_relation, primary_key, exclude_columns)) }}
{% endmacro %}

{% macro default__compare_which_columns_differ(a_relation, b_relation, primary_key, exclude_columns=[]) %}  

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=a_relation, except=exclude_columns) %}

with bool_or as (

    select 
        true as anchor
        {% for column in column_names %}
            {% set column_name = adapter.quote(column) %}
            {% set compare_statement %}
                ((a.{{ column_name }} != b.{{ column_name }})
                or (a.{{ column_name }} is null and b.{{ column_name }} is not null)
                or (a.{{ column_name }} is not null and b.{{ column_name }} is null))
            {% endset %}
        
        , {{ dbt.bool_or(compare_statement) }} as {{ column | lower }}_has_difference
    
        {% endfor %}
    from {{ a_relation }} as a
    inner join {{ b_relation }} as b
        on a.{{ primary_key }} = b.{{ primary_key }}

)

{% for column in column_names %}
    
    select 
        '{{ column }}' as column_name, 
        {{ column | lower }}_has_difference as has_difference
    
    from bool_or

    {% if not loop.last %}
        
    union all 

    {% endif %}

{% endfor %}

{% endmacro %}
