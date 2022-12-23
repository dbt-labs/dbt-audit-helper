{% macro is_cloud() %}
    {# In the dbt Cloud context, `DBT_ENV` will always resolve to "prod" #}
    {% set is_cloud = env_var("DBT_ENV", "core") == "prod" %}
    {{ return(is_cloud) }}
{% endmacro %}

{% macro cloud_safe_print_table(audit_results) %}
{% if execute %}
    {% if is_cloud() %}
        {% do log(audit_results.column_names, info=True) %}
        {% for row in audit_results.rows %}
            {% do log(row.values(), info=True) %}
        {% endfor %}
    {% else %}
        {% do audit_results.print_table() %}
        {{ log("", info=True) }}
    {% endif %}
{% endif %}
{% endmacro %}