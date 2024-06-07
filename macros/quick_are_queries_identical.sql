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
    {% if execute %}
        {# Need to only throw this error when the macro is actually trying to be used, not during intial parse phase #}
        {# if/when unit tests get support for `enabled` config, this check can be removed as they won't be supplied for parse anyway #}
        {% do exceptions.raise_compiler_error("quick_are_queries_identical() is not implemented for adapter '"~ target.type ~ "'" ) %}
    {% endif %}
{% endmacro %}

{% macro bigquery__quick_are_queries_identical(query_a, query_b, columns, event_time) %}
    {% set joined_cols = columns | join(", ") %}
    {% if event_time %}
        {% set event_time_props = audit_helper._get_comparison_bounds(a_query, b_query, event_time) %}
    {% endif %}

    with query_a as (
        select {{ joined_cols }}
        from ({{ query_a }})
        {{ audit_helper.event_time_filter(event_time_props) }}
    ), 
    query_b as (
        select {{ joined_cols }}
        from ({{ query_b }})
        {{ audit_helper.event_time_filter(event_time_props) }}
    )

    select count(distinct hash_result) = 1 as are_tables_identical
    from (
        select bit_xor(farm_fingerprint(to_json_string(query_a))) as hash_result
        from query_a

        union all
        
        select bit_xor(farm_fingerprint(to_json_string(query_b))) as hash_result
        from query_b
    ) as hashes
{% endmacro %}

{% macro snowflake__quick_are_queries_identical(query_a, query_b, columns, event_time) %}
    {% set joined_cols = columns | join(", ") %}
    {% if event_time %}
        {% set event_time_props = audit_helper._get_comparison_bounds(a_query, b_query, event_time) %}
    {% endif %}

    select count(distinct hash_result) = 1 as are_tables_identical
    from (
        select hash_agg({{ joined_cols }}) as hash_result
        from ({{ query_a }}) query_a_subq
        {{ audit_helper.event_time_filter(event_time_props) }}

        union all
        
        select hash_agg({{ joined_cols }}) as hash_result
        from ({{ query_b }}) query_b_subq
        {{ audit_helper.event_time_filter(event_time_props) }}

    ) as hashes
{% endmacro %}