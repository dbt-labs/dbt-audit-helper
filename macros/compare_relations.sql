{% macro pop_columns(columns, columns_to_pop) %}
{% set popped_columns=[] %}

{% for column in columns %}
    {% if column.name | lower not in columns_to_pop | lower %}
        {% do popped_columns.append(column) %}
    {% endif %}
{% endfor %}

{{ return(popped_columns) }}
{% endmacro %}


----

{% macro compare_relations(a_relation, b_relation, exclude_columns=[], primary_key=None) %}

{%- set a_columns = adapter.get_columns_in_relation(a_relation) -%}

{% set check_columns=audit_helper.pop_columns(a_columns, exclude_columns) %}

{% set check_cols_csv = check_columns | map(attribute='quoted') | join(', ') %}


with a as (

    select
        {{ check_cols_csv }}

    from {{ a_relation }}

),

b as (

    select
        {{ check_cols_csv }}

    from {{ b_relation }}

),

a_intersect_b as (

    select * from a
    {{ dbt_utils.intersect() }}
    select * from b

),

a_except_b as (

    select * from a
    {{ dbt_utils.except() }}
    select * from b

),

b_except_a as (

    select * from b
    {{ dbt_utils.except() }}
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

summary_stats as (
    select
        in_a,
        in_b,
        count(*) as count
    from all_records

    group by 1, 2
)
-- select * from all_records
-- where not (in_a and in_b)
-- order by {{ primary_key ~ ", " if primary_key is not none }} in_a desc, in_b desc

select * from summary_stats

order by in_a desc, in_b desc

{% endmacro %}
