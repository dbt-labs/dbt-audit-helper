/*
As described by the Infinite Lambda team here: https://infinitelambda.com/data-validation-refactoring-snowflake/

Some platforms let you take a hash of the whole table, which can be very very fast compared to comparing each row. 

If you run this and it returns false, you still have to run the more in-depth queries to find out what specific changes there are, 
but it's a good way to quickly verify identical results if that's what you're expecting. 
*/

{% macro quick_are_queries_identical(query_a, query_b, columns=[], event_time=None) %}
    {{ return (adapter.dispatch('quick_are_queries_identical', 'audit_helper')(query_a, query_b, columns, event_time)) }}
{% endmacro %}

{% macro default__quick_are_queries_identical(query_a, query_b, columns, event_time) %}
    {% set joined_cols = columns | join(", ") %}
    {% if event_time %}
        {% set event_time_props = audit_helper.get_comparison_bounds(a_query, b_query, event_time) %}
    {% endif %}

    select count(distinct hash_result) = 1 as are_tables_identical
    from (
        select hash_agg({{ joined_cols }}) as hash_result
        from ({{ query_a }})
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}

        union all
        
        select hash_agg({{ joined_cols }}) as hash_result
        from ({{ query_b }})
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}

    ) as hashes
{% endmacro %}

{% macro is_quick_are_queries_identical_supported() %}
    {{ return (adapter.dispatch('is_quick_are_queries_identical_supported', 'audit_helper')()) }}
{% endmacro %}

{% macro default__is_quick_are_queries_identical_supported() %}
    {{ return (False) }}
{% endmacro %}

{% macro snowflake__is_quick_are_queries_identical_supported() %}
    {{ return (True) }}
{% endmacro %}