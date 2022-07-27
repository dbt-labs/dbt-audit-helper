{% macro compare_column_values_count(a_query, b_query, primary_key, column_to_compare, updated_at_column) -%}
  {{ return(adapter.dispatch('compare_column_values_count', 'audit_helper')(a_query, b_query, primary_key, column_to_compare, updated_at_column)) }}
{%- endmacro %}

{% macro default__compare_column_values_count(a_query, b_query, primary_key, column_to_compare, updated_at_column) -%}
with a_query as (
    {{ a_query }}
),

b_query as (
    {{ b_query }}
),

joined as (
    select
        coalesce(a_query.{{ primary_key }}, b_query.{{ primary_key }}) as {{ primary_key }},
        a_query.{{ column_to_compare }} as a_query_value,
        b_query.{{ column_to_compare }} as b_query_value,
        a_query.{{ column_to_compare }} = b_query.{{ column_to_compare }} as perfect_match,
        a_query.{{ column_to_compare }} is null as null_in_a,
        b_query.{{ column_to_compare }} is null as null_in_b,
        a_query.{{ primary_key }} is null as missing_from_a,
        b_query.{{ primary_key }} is null as missing_from_b,
        a_query.{{ column_to_compare }} != b_query.{{ column_to_compare }} and 
          (a_query.{{ column_to_compare }} is not null or b_query.{{ column_to_compare }} is not null)
          as conflicting_values,
           -- considered a conflict if the values do not match AND at least one of the values is not null
        a_query.{{ updated_at_column }} as updated_at_a,
        b_query.{{ updated_at_column }} as updated_at_b

    from a_query

    full outer join b_query on a_query.{{ primary_key }} = b_query.{{ primary_key }}
)
    select
        '{{ column_to_compare }}' as column_name,
        count(*) as count_records,
        sum(case when perfect_match then 1 else 0 end) as perfect_match_count,

        sum(case when null_in_a then 1 else 0 end) as null_in_a,

        sum(case when null_in_b then 1 else 0 end) as null_in_b,

        sum(case when missing_from_a then 1 else 0 end) as missing_from_a,
        min(case when missing_from_a then updated_at_b end) as earliest_missing_from_a, 
        -- use updated_at from b to state earliest missing value from a

        sum(case when missing_from_b then 1 else 0 end) as missing_from_b,
        min(case when missing_from_b then updated_at_a end) as earliest_missing_from_b, 
        -- use updated_at from a to state earliest missing value from b

        sum(case when conflicting_values then 1 else 0 end) as conflicting_values,
        min(case when conflicting_values then updated_at_a end) as earliest_conflicting_values_a,
        min(case when conflicting_values then updated_at_b end) as earliest_conflicting_values_b

    from joined

    group by column_name

{% endmacro %}
