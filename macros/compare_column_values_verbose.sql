{% macro compare_column_values_verbose(a_query, b_query, primary_key, column_to_compare) -%}
  {{ return(adapter.dispatch('compare_column_values_verbose', 'audit_helper')(a_query, b_query, primary_key, column_to_compare)) }}
{%- endmacro %}


{% macro default__compare_column_values_verbose(a_query, b_query, primary_key, column_to_compare) -%}
with a_query as (
    {{ a_query }}
),

b_query as (
    {{ b_query }}
)
    select
        coalesce(a_query.{{ primary_key }}, b_query.{{ primary_key }}) as primary_key,

        {% if target.name == 'postgres' or target.name == 'redshift' %}
            '{{ column_to_compare }}'::text as column_name,
        {% else %}
            '{{ column_to_compare }}' as column_name,
        {% endif %}

        coalesce(a_query.{{ column_to_compare }} = b_query.{{ column_to_compare }},
          (a_query.{{ column_to_compare }} is null and b_query.{{ column_to_compare }} is null),
          false) as perfect_match,
        a_query.{{ column_to_compare }} is null and a_query.{{ primary_key }} is not null as null_in_a,
        b_query.{{ column_to_compare }} is null and b_query.{{ primary_key }} is not null as null_in_b,
        a_query.{{ primary_key }} is null as missing_from_a,
        b_query.{{ primary_key }} is null as missing_from_b,
        coalesce(a_query.{{ column_to_compare }} != b_query.{{ column_to_compare }} and
            (a_query.{{ column_to_compare }} is not null or b_query.{{ column_to_compare }} is not null), false)
          as conflicting_values
           -- considered a conflict if the values do not match AND at least one of the values is not null.

    from a_query

    full outer join b_query on (a_query.{{ primary_key }} = b_query.{{ primary_key }})



{% endmacro %} 
