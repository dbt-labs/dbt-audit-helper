{% macro compare_which_query_columns_differ(a_query, b_query, primary_key_columns=[], columns=[], event_time=None) %}
    {{ return(adapter.dispatch('compare_which_query_columns_differ', 'audit_helper')(a_query, b_query, primary_key_columns, columns, event_time)) }}
{% endmacro %}

{% macro default__compare_which_query_columns_differ(a_query, b_query, primary_key_columns, columns, event_time) %}
    {% set columns = audit_helper._ensure_all_pks_are_in_column_set(primary_key_columns, columns) %}
    {% if event_time %}
        {% set event_time_props = audit_helper._get_comparison_bounds(event_time) %}
    {% endif %}

    {% set joined_cols = columns | join (", ") %}

        with a as (
            select 
                {{ joined_cols }},
                {{ audit_helper._generate_null_safe_surrogate_key(primary_key_columns) }} as dbt_audit_surrogate_key
            from ({{ a_query }}) as a_subq
            {{ audit_helper.event_time_filter(event_time_props) }}
        ),
        b as (
            select 
                {{ joined_cols }},
                {{ audit_helper._generate_null_safe_surrogate_key(primary_key_columns) }} as dbt_audit_surrogate_key
            from ({{ b_query }}) as b_subq
            {{ audit_helper.event_time_filter(event_time_props) }}
        ),

        calculated as (
            select 
                {% for column in columns %}
                    {% set quoted_column = adapter.quote(column) %}
                    {% set compare_statement %}
                        (
                            (a.{{ quoted_column }} != b.{{ quoted_column }})
                            or (a.{{ quoted_column }} is null and b.{{ quoted_column }} is not null)
                            or (a.{{ quoted_column }} is not null and b.{{ quoted_column }} is null)
                        )
                    {% endset %}
                
                {{ dbt.bool_or(compare_statement) }} as {{ column | lower }}_has_difference

                {%- if not loop.last %}, {% endif %}
                {% endfor %}
            from a
            inner join b on a.dbt_audit_surrogate_key = b.dbt_audit_surrogate_key
        )

    {% for column in columns %}
    
    select 
        '{{ column }}' as column_name, 
        {{ column | lower }}_has_difference as has_difference
    
    from calculated

    {% if not loop.last %}
        
    union all 

    {% endif %}

    {% endfor %}

{% endmacro %}
