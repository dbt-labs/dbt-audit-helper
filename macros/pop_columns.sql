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