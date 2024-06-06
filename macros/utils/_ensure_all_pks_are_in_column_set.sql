{# If someone forgot to include the PK columns in their main set of columns, fix it up for them #}
{# Assuming that the PKs are the most important columns, so they go to the front of the list #}

{% macro _ensure_all_pks_are_in_column_set(primary_key_columns, columns) %}
    {% set lower_cols = columns | map('lower') | list %}
    {% set missing_pks = [] %}

    {% for pk in primary_key_columns %}
        {% if pk | lower not in lower_cols %}
            {% do missing_pks.append(pk) %}
        {% endif %}
    {% endfor %}

    {% if missing_pks | length > 0 %}
        {% set columns = missing_pks + columns %}
    {% endif %}
    
    {% do return (columns) %}
{% endmacro %}