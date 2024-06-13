{% macro compare_and_classify_relation_rows(a_relation, b_relation, primary_key_columns=[], columns=None, event_time=None, sample_limit=20) %}
    {%- if not columns -%}
        {%- set columns = audit_helper._get_intersecting_columns_from_relations(a_relation, b_relation) -%}
    {%- endif -%}

    {{ 
        audit_helper.compare_and_classify_query_results(
            "select * from " ~ a_relation,
            "select * from " ~ b_relation,
            primary_key_columns,
            columns,
            event_time,
            sample_limit
        )
    }}
{% endmacro %}