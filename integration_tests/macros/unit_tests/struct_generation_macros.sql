{%- macro _basic_json_function() -%}
    {%- if target.type == 'snowflake' -%}
        object_construct
    {%- elif target.type == 'bigquery' -%}
        json_object
    {%- elif target.type == 'databricks' -%}
        map
    {%- elif target.type == 'redshift' -%}
        json_build_object
    {%- else -%}
        {%- do exceptions.raise_compiler_error("Unknown adapter '"~ target.type ~ "'")-%}
    {%- endif -%}
{%- endmacro -%}