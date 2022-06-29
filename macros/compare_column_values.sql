{% macro compare_column_values(a_query, b_query, primary_key, column_to_compare) -%}
  {{ return(adapter.dispatch('compare_column_values', 'audit_helper')(a_query, b_query, primary_key, column_to_compare)) }}
{%- endmacro %}

{% macro default__compare_column_values(a_query, b_query, primary_key, column_to_compare) -%}
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
        case
            when a_query.{{ column_to_compare }} = b_query.{{ column_to_compare }} then '✅: perfect match'
            when a_query.{{ column_to_compare }} is null and b_query.{{ column_to_compare }} is null then '✅: both are null'
            when a_query.{{ primary_key }} is null then '🤷: ‍missing from a'
            when b_query.{{ primary_key }} is null then '🤷: missing from b'
            when a_query.{{ column_to_compare }} is null then '🤷: value is null in a only'
            when b_query.{{ column_to_compare }} is null then '🤷: value is null in b only'
            when a_query.{{ column_to_compare }} != b_query.{{ column_to_compare }} then '🙅: ‍values do not match'
            else 'unknown' -- this should never happen
        end as match_status,
        case
            when a_query.{{ column_to_compare }} = b_query.{{ column_to_compare }} then 0
            when a_query.{{ column_to_compare }} is null and b_query.{{ column_to_compare }} is null then 1
            when a_query.{{ primary_key }} is null then 2
            when b_query.{{ primary_key }} is null then 3
            when a_query.{{ column_to_compare }} is null then 4
            when b_query.{{ column_to_compare }} is null then 5
            when a_query.{{ column_to_compare }} != b_query.{{ column_to_compare }} then 6
            else 7 -- this should never happen
        end as match_order

    from a_query

    full outer join b_query on a_query.{{ primary_key }} = b_query.{{ primary_key }}
),

aggregated as (
    select
        '{{ column_to_compare }}' as column_name,
        match_status,
        match_order,
        count(*) as count_records
    from joined

    group by column_name, match_status, match_order
)

select
    column_name,
    match_status,
    count_records,
    round(100.0 * count_records / sum(count_records) over (), 2) as percent_of_total

from aggregated

order by match_order

{% endmacro %}
