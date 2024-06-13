{% macro compare_which_relation_columns_differ(a_relation, b_relation, primary_key_columns=[], columns=[], event_time=None) %}
    {%- if not columns -%}
        {%- set columns = audit_helper._get_intersecting_columns_from_relations(a_relation, b_relation) -%}
    {%- endif -%}

    {{ 
        audit_helper.compare_which_query_columns_differ(
            "select * from " ~ a_relation,
            "select * from " ~ b_relation,
            primary_key_columns,
            columns,
            event_time
        )
    }}
{% endmacro %}