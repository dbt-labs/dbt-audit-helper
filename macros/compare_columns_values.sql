{% macro compare_columns_values(source_query, target_query, primary_key, columns_to_compare, add_order_by=true, source_name="source", target_name="target", renamed_columns_map={}) -%}
  {{
      return(
          adapter.dispatch('compare_columns_values')
          (source_query, target_query, primary_key, columns_to_compare, add_order_by, source_name, target_name, renamed_columns_map)
        )
    }}
{%- endmacro %}

{% macro create_joined(primary_key, column_to_compare, source_name, target_name, target_column_name=None) %}
    {%- set target_column_name = target_column_name or column_to_compare -%}
    {%- if column_to_compare != target_column_name -%}
            {%- set column_name = column_to_compare + " → " + target_column_name + " (renamed)" -%}
        {%- else -%}
            {%- set column_name = column_to_compare -%}
    {%- endif -%}
    select
        '{{ column_name }}' as column_name,
        coalesce(source_query.{{ primary_key }}, target_query.{{ primary_key }}) as {{ primary_key }},
        cast(source_query.{{ column_to_compare }} as varchar) as source_query_value,
        cast(target_query.{{ target_column_name }} as varchar) as target_query_value,
        case
            when source_query.{{ column_to_compare }} = target_query.{{ target_column_name }} then 1
            when source_query.{{ column_to_compare }} is null and target_query.{{ target_column_name }} is null then 2
            when source_query.{{ primary_key }} is null then 3
            when target_query.{{ primary_key }} is null then 4
            when source_query.{{ column_to_compare }} is null then 5
            when target_query.{{ target_column_name }} is null then 6
            when source_query.{{ column_to_compare }} != target_query.{{ target_column_name }} then 7
            else 8 -- this should never happen
        end as match_order

    from source_query

        full outer join target_query
            on source_query.{{ primary_key }} = target_query.{{ primary_key }}
{% endmacro %}

{% macro default__compare_columns_values(source_query, target_query, primary_key, columns_to_compare, add_order_by=true, source_name="source", target_name="target", renamed_columns_map={}) -%}

with source_query as (
    {{ source_query }}
), target_query as (
    {{ target_query }}
), match_status_order as (
    select
        match_order,
        match_status
    from
        (
            values
                (1, '✅: perfect match'),
                (2, '✅: both are null'),
                (3, '🤷: ‍missing from {{ source_name }}'),
                (4, '🤷: missing from {{ target_name }}'),
                (5, '🤷: value is null in {{ source_name }} only'),
                (6, '🤷: value is null in {{ target_name }} only'),
                (7, '🙅: ‍values do not match'),
                (8, 'unknown')
        ) as a(match_order, match_status)
), joined as (
    {% for column_to_compare in columns_to_compare %}
        {%- if column_to_compare in renamed_columns_map -%}
            {%- set target_column_name = renamed_columns_map[column_to_compare] -%}
        {%- else -%}
            {%- set target_column_name = column_to_compare -%}
        {%- endif -%}
        {{ create_joined(primary_key, column_to_compare, source_name, target_name, target_column_name) }}
        {%- if not loop.last %}
        UNION ALL
        {% endif -%}
    {% endfor %}
), aggregated as (
    select
        j.column_name,
        mso.match_status,
        j.match_order,
        count(1) as count_records
    from
        joined as j

        inner join match_status_order as mso
            on mso.match_order = j.match_order
    group by
        j.column_name,
        mso.match_status,
        j.match_order
)
select
    column_name,
    match_status,
    match_order,
    count_records,
    round(
        100.0
        * count_records
        / sum(count_records) over (partition by column_name),
        2
    ) as percent_of_total
from
    aggregated
{% if add_order_by %}
order by
    column_name,
    match_order
{% endif %}

{% endmacro %}