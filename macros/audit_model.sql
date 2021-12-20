{% macro audit_model(model_name, unique_key, prod_schema="prod", source_name="source", target_name="target", renamed_columns_map={}, add_order_by=true) %}
{%- set relation = ref(model_name) -%}
{%- set prod_model = relation | replace(target.schema, prod_schema) -%}
{%- set this_columns = adapter.get_columns_in_relation(model_name) -%}
{%- set prod_columns = adapter.get_columns_in_relation(prod_model) -%}
{%- set prod_columns_names = [] -%}
{%- set renamed_prod_columns = [] -%}
{%- for prod_column in prod_columns -%}
    {%- set prod_columns = prod_columns_names.append(prod_column.name) -%}
{%- endfor -%}
{%- set matching_columns = [] -%}
{%- set missing_columns = [] -%}
{%- for this_column in this_columns -%}
    {%- if this_column in prod_columns -%}
        {%- set matching_columns = matching_columns.append(this_column.name) -%}
    {% elif this_column.name in renamed_columns_map %}
        {%- if renamed_columns_map[this_column.name] in prod_columns_names -%}
            {%- set matching_columns = matching_columns.append(this_column.name) -%}
            {%- set renamed_prod_columns = renamed_prod_columns.append(renamed_columns_map[this_column.name]) -%}
        {%- endif -%}
    {%- else -%}
        {%- set missing_columns = missing_columns.append([this_column.name, "⛔️  missing from " + target_name]) -%}
    {%- endif -%}
{%- endfor -%}
{%- for prod_column in prod_columns -%}
    {%- if prod_column.name not in matching_columns -%}
        {%- if prod_column.name not in renamed_prod_columns -%}
            {%- set missing_columns = missing_columns.append([prod_column.name, "⛔️  missing from " + source_name]) -%}
        {%- endif -%}
    {%- endif -%}
{%- endfor -%}
{%- set this_query -%}
    select * from {{ ref(model_name) }}
{%- endset -%}

{%- set prod_query -%}
    select * from {{ prod_model }}
{%- endset -%}

{%- set audit_query = compare_columns_values(
    source_query=this_query,
    target_query=prod_query,
    primary_key=unique_key,
    columns_to_compare=matching_columns,
    add_order_by=false,
    source_name=source_name,
    target_name=target_name,
    renamed_columns_map=renamed_columns_map
) -%}
with __cte as (
{{ audit_query }}
{% if missing_columns %}
union all
select
    *,
    1 as match_order,
    null as count_records,
    null as percent_of_total
from
    (
        values
    {% for missing_column in missing_columns %}
        ('{{ missing_column[0] }}', '{{ missing_column[1] }}')
        {%- if not loop.last -%}
        ,
        {%- endif -%}
    {% endfor %}
    ) as a (column_name, match_status)
{% endif %}
)
select
    column_name,
    match_status,
    {% if not add_order_by %}
    match_order,
    {% endif %}
    count_records,
    percent_of_total
from
    __cte
{% if add_order_by %}
order by
    column_name,
    match_order
{% endif %}

{% endmacro %}
