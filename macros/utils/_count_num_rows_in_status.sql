{% macro _count_num_rows_in_status() %}
    {{ return(adapter.dispatch('_count_num_rows_in_status', 'audit_helper')()) }}
{% endmacro %}

{%- macro default___count_num_rows_in_status() -%}
    count(distinct dbt_audit_surrogate_key, dbt_audit_pk_row_num) over (partition by dbt_audit_row_status)
{% endmacro %}

{%- macro bigquery___count_num_rows_in_status() -%}
    count(distinct {{ dbt.concat(["dbt_audit_surrogate_key", "dbt_audit_pk_row_num"]) }}) over (partition by dbt_audit_row_status)
{% endmacro %}

{%- macro postgres___count_num_rows_in_status() -%}
    {{ audit_helper._count_num_rows_in_status_without_distinct_window_func() }}
{% endmacro %}

{%- macro databricks___count_num_rows_in_status() -%}
    {{ audit_helper._count_num_rows_in_status_without_distinct_window_func() }}
{% endmacro %}

{% macro _count_num_rows_in_status_without_distinct_window_func() %}
    {#- Some platforms don't support count(distinct) inside of window functions -#}
    {#- You can get the same outcome by dense_rank, assuming no nulls (we've already handled that) #}
    {# https://stackoverflow.com/a/22347502 -#}
    dense_rank() over (partition by dbt_audit_row_status order by dbt_audit_surrogate_key, dbt_audit_pk_row_num)
    + dense_rank() over (partition by dbt_audit_row_status order by dbt_audit_surrogate_key desc, dbt_audit_pk_row_num desc)
    - 1
{% endmacro %}