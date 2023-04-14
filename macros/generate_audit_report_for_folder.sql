{% macro generate_audit_report_for_folder(model_folder_path, compare_database, compare_schema, identifiers = {}, primary_keys = {}, exclude_columns = {}, audit_helper_macro_name = 'compare_relations') %}

    {%- set re = modules.re -%}

    {#- *----- Create a list to hold the model nodes -----* -#}
    {%- set model_list = [] %}

    {#- *-------------------------------------------------
        * Get the list of models in the resource 
        * path and add them to the list 
    ------------------------------------------------------* -#}
    {%- if execute %}
        {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}
            {% set regex_folder_pattern = 'models/' ~ model_folder_path ~ '/.*' %}
            {%- if re.match(regex_folder_pattern, node.original_file_path) is not none %}
                {%- do model_list.append(node.name) %}
            {%- endif %}
        {%- endfor %}
    {%- endif %}

    {{ audit_helper.generate_audit_report(
        model_list = model_list, 
        compare_database = compare_database, 
        compare_schema = compare_schema,
        identifiers = identifiers, 
        primary_keys = primary_keys, 
        exclude_columns = exclude_columns, 
        audit_helper_macro_name = audit_helper_macro_name
    ) }}
    
{% endmacro %}