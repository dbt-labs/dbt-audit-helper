{# Taken from https://github.com/dbt-labs/dbt-utils/blob/main/macros/sql/generate_surrogate_key.sql but without the option to treat nulls as empty strings #}

{%- macro _generate_null_safe_surrogate_key(field_list) -%}
    {{ return(adapter.dispatch('_generate_null_safe_surrogate_key', 'audit_helper')(field_list)) }}
{% endmacro %}

{%- macro default___generate_null_safe_surrogate_key(field_list) -%}

{%- set fields = [] -%}

{%- for field in field_list -%}

    {%- do fields.append(
        "coalesce(cast(" ~ field ~ " as " ~ dbt.type_string() ~ "), '_dbt_audit_helper_surrogate_key_null_')"
    ) -%}

    {%- if not loop.last %}
        {%- do fields.append("'-'") -%}
    {%- endif -%}

{%- endfor -%}

{{ dbt.hash(dbt.concat(fields)) }}

{%- endmacro -%}