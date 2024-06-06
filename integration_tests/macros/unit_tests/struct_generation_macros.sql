{%- macro _basic_json_function() -%}
    {%- if target.type == 'snowflake' -%}
        object_construct
    {%- elif target.type == 'bigquery' -%}
        json_object
    {%- elif target.type == 'databricks' -%}
        map
    {%- elif execute -%}
        {# Only raise exception if it's actually being called, not during parsing #}
        {%- do exceptions.raise_compiler_error("Unknown adapter '"~ target.type ~ "'") -%}
    {%- endif -%}
{%- endmacro -%}

{% macro _complex_json_function(json) %}

    {% if target.type == 'redshift' %}
        json_parse({{ json }})
    {% elif target.type == 'databricks' %}
        from_json({{ json }}, schema_of_json({{ json }}))
    {% elif target.type in ['snowflake', 'bigquery'] %}
        parse_json({{ json }})
    {% elif execute %}
        {# Only raise exception if it's actually being called, not during parsing #}
        {%- do exceptions.raise_compiler_error("Unknown adapter '"~ target.type ~ "'") -%}    
    {% endif %}
{% endmacro %}