{% macro generate_audit_report(model_list, compare_database, compare_schema, identifiers = {}, primary_keys = {}, exclude_columns = {}, audit_helper_macro_name = 'compare_relations') %}

    {% set compatible_audit_helper_macros = ['compare_relations', 'compare_all_columns', 'compare_relation_columns'] %}
    {% if audit_helper_macro_name not in compatible_audit_helper_macros %}
        {{ exceptions.raise_compiler_error("Invalid audit_helper_macro input. Acceptable inputs: " ~ compatible_audit_helper_macros) }}
    {% endif %}

    {% if audit_helper_macro_name == 'compare_all_columns' and not primary_keys|length %}
        {{ exceptions.raise_compiler_error("Audit helper macro compare_all_columns requires primary_keys input.") }}
    {% endif %}

    {% if audit_helper_macro_name == 'compare_relation_columns' %}
        {% if exclude_columns|length %}
            {{ exceptions.raise_compiler_error("Audit helper macro compare_relation_columns does not accept exclude_columns input.") }}
        {% elif primary_keys|length %}
            {{ exceptions.raise_compiler_error("Audit helper macro compare_relation_columns does not accept primary_keys input.") }}
        {% endif %}
    {% endif %}

    {%- set model_node_list = [] %}

    {%- if execute %}
        {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
            
            {%- if node.name in model_list %}
                {%- do model_node_list.append(node) %}
            {%- endif %}

        {%- endfor %}
    {%- endif %}

    with
    
    {% for current_model in model_node_list %}
    {{ current_model.name }}_results as (
        {#- *---------------------------------------------
            * This is where the audit_helper macro is used!
        --------------------------------------------------* -#}

        {% if identifiers[current_model.name] %}
            {% set legacy_model_name = identifiers[current_model.name] %}
        {% else %}
            {% set legacy_model_name = current_model.name %}
        {% endif %}

        {% set compare_relation = adapter.get_relation(
            database = compare_database,
            schema = compare_schema,
            identifier = legacy_model_name
        ) -%}

        {% set dbt_relation = adapter.get_relation(
            database = current_model.database,
            schema = current_model.schema,
            identifier = current_model.name
        ) -%}

        {% set audit_helper_macro = audit_helper.get(audit_helper_macro_name) %}

        {% set model_primary_key = primary_keys[current_model.name] %}

        {% set model_exclude_columns = exclude_columns[current_model.name] %}

        {% if not compare_relation %}

            {{ exceptions.raise_compiler_error("Compare relation does not exist - confirm you have supplied the correct compare_database, compare_schema, and identifiers.") }} 

        {% elif dbt_relation is none %}

            {{ exceptions.raise_compiler_error("dbt model has not been materialized in the warehouse - try running a dbt build for model " ~ current_model.name) }} 
        
        {% else %}

            {% do log("Compare relation (a) " ~ compare_relation ~ " to dbt model (b) " ~ dbt_relation, True) %}
    
            {% if audit_helper_macro_name != 'compare_relation_columns' %}
                {{ audit_helper_macro(a_relation = compare_relation, b_relation = dbt_relation, primary_key = model_primary_key, exclude_columns = model_exclude_columns) }}
            {% else %}
                {{ audit_helper_macro(a_relation = compare_relation, b_relation = dbt_relation) }}
            {% endif %}

        {% endif %}

    ),
    {% endfor %}

    final as (
        {% for current_model in model_node_list %}
        select 
            '{{ current_model.name }}' as audit_model, 
            * from {{ current_model.name }}_results
        {% if not loop.last %}union all{% endif %}
        {% endfor %}
    )
    
    select * from final
    order by audit_model
    
{% endmacro %}