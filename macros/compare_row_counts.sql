{% macro compare_row_counts(a_relation, b_relation) %}
  {{ return(adapter.dispatch('compare_row_counts', 'audit_helper')(a_relation, b_relation)) }}
{% endmacro %}

{% macro default__compare_row_counts(a_relation, b_relation) %}

        select
            '{{ a_relation }}' as relation_name,
            count(*) as total_records
        from {{ a_relation }}

        union all

        select
            '{{ b_relation }}' as relation_name,
            count(*) as total_records
        from {{ b_relation }}
  
{% endmacro %}