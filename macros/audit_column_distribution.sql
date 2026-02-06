{% macro audit_column_distribution(
    relation_a,
    relation_b,
    column_name,
    buckets=10
) %}

with a as (
    select {{ column_name }} as val, count(*) as cnt
    from {{ relation_a }}
    group by {{ column_name }}
),
b as (
    select {{ column_name }} as val, count(*) as cnt
    from {{ relation_b }}
    group by {{ column_name }}
),
combined as (
    select
        coalesce(a.val, b.val) as value,
        coalesce(a.cnt, 0) as count_a,
        coalesce(b.cnt, 0) as count_b,
        abs(coalesce(a.cnt, 0) - coalesce(b.cnt, 0)) as diff
    from a
    full outer join b
        on a.val = b.val
)
select *
from combined
order by diff desc

{% endmacro %}
