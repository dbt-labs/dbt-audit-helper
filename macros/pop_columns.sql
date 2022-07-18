{% macro pop_columns(columns, columns_to_pop) %}
{% set popped_columns=[] %}

{% for column in columns %}
    {% if column.name | lower not in columns_to_pop | lower %}
        {% do popped_columns.append(column) %}
    {% endif %}
{% endfor %}

{{ return(popped_columns) }}
{% endmacro %}