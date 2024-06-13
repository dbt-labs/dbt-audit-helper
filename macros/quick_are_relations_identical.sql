{% macro quick_are_relations_identical(a_relation, b_relation, columns=None, event_time=None) %}
    {% if not columns %}
        {% set columns = audit_helper._get_intersecting_columns_from_relations(a_relation, b_relation) %}
    {% endif %}

    {{
        audit_helper.quick_are_queries_identical(
            "select * from " ~ a_relation,
            "select * from " ~ b_relation,
            columns, 
            event_time
        )
    }}
{% endmacro %}