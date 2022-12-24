{% macro is_cloud() %}
    {# In the dbt Cloud context, `DBT_ENV` will always resolve to "prod" #}
    {% set is_cloud = env_var("DBT_ENV", "core") == "prod" %}
    {{ return(is_cloud) }}
{% endmacro %}

{% macro cloud_safe_print_table(audit_results) %}
{% if execute %}
    {% if is_cloud() %}
        {% set header = [] %}
        {% for i in range(0,audit_results.column_names|length) %}
            {{ header.append(audit_results.column_names[i]) }}
        {% endfor %}
        {% do log('| ' ~ header|join(' | ') ~ ' |', info=True) %}
        {# do log(audit_results.column_names, info=True) #}
        {% for row in audit_results.rows %}
            {% set clean_row = [''] %}
            {% for val in row.values() %}
                {{ clean_row.append(val) }}
            {% endfor %}
            {% do log(clean_row|join(' | ') ~ ' |', info=True) %}
            {# do log(row.values(), info=True) #}
        {% endfor %}
    {% else %}
        {% do audit_results.print_table() %}
        {{ log("", info=True) }}
    {% endif %}
{% endif %}
{% endmacro %}