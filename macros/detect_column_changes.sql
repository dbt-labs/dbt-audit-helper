{% macro detect_column_changes(a_relation, b_relation, primary_key, exclude_columns=[]) %}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=a_relation, except=exclude_columns) %}

with bool_or as (

    select 
        true as anchor
        {% for column in column_names %}
            {% set compare_statement %}
                (a.{{ column | lower }} != b.{{ column | lower }}
                or a.{{ column | lower }} is null and b.{{ column | lower }} is not null
                or a.{{ column | lower }} is not null and b.{{ column | lower }} is null)
            {% endset %}
        
        , {{ dbt.bool_or(compare_statement) }} as {{ column | lower }}_is_changed
    
        {% endfor %}
    from {{ a_relation }} as a
    inner join {{ b_relation }} as b
        on a.{{ primary_key }} = b.{{ primary_key }}

)

{% for column in column_names %}

    select 
        '{{ column | lower }}' as column_name, 
        {{ column | lower }}_is_changed as is_changed 
    
    from bool_or

    {% if not loop.last %}
        
    union all 

    {% endif %}

{% endfor %}

{% endmacro %}
