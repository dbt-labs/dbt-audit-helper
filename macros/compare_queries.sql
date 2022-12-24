{% macro compare_queries(a_query, b_query, primary_key=None, summarize=true) -%}
  {{ return(adapter.dispatch('compare_queries', 'audit_helper')(a_query, b_query, primary_key, summarize)) }}
{%- endmacro %}

{% macro default__compare_queries(a_query, b_query, primary_key=None, summarize=true) %}

with a as (

    {{ a_query }}

),

b as (

    {{ b_query }}

),

a_intersect_b as (

    select * from a
    {{ dbt.intersect() }}
    select * from b

),

a_except_b as (

    select * from a
    {{ dbt.except() }}
    select * from b

),

b_except_a as (

    select * from b
    {{ dbt.except() }}
    select * from a

),

all_records as (

    select
        *,
        true as in_a,
        true as in_b
    from a_intersect_b

    union all

    select
        *,
        true as in_a,
        false as in_b
    from a_except_b

    union all

    select
        *,
        false as in_a,
        true as in_b
    from b_except_a

),

{%- if summarize %}

summary_stats as (

    select

        in_a,
        in_b,
        count(*) as count

    from all_records
    group by 1, 2

),

final as (

    select

        *,
        round(100.0 * count / sum(count) over (), 2) as percent_of_total

    from summary_stats
    order by in_a desc, in_b desc

)

{%- else %}

final as (
    
    select * from all_records
    where not (in_a and in_b)
    order by {{ primary_key ~ ", " if primary_key is not none }} in_a desc, in_b desc

)

{%- endif %}

select * from final

{% endmacro %}
