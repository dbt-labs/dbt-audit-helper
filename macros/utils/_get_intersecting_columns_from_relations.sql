{% macro _get_intersecting_columns_from_relations(a_relation, b_relation) %}        
    {%- set a_cols = dbt_utils.get_filtered_columns_in_relation(a_relation) -%}
    {%- set b_cols = dbt_utils.get_filtered_columns_in_relation(b_relation) -%}
    
    {%- set intersection = [] -%}
    {%- for col in a_cols -%}
        {%- if col in b_cols -%}
            {%- do intersection.append(col) -%}
        {%- endif -%}
    {%- endfor -%}

    {% do return(intersection) %}
{% endmacro %}