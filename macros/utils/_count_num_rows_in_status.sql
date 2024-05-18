{% macro _count_num_rows_in_status() %}
    {{ return(adapter.dispatch('_count_num_rows_in_status', 'audit_helper')()) }}
{% endmacro %}

{%- macro default___count_num_rows_in_status() -%}
    count(distinct dbt_audit_surrogate_key, dbt_audit_pk_row_num) over (partition by dbt_audit_row_status)
{% endmacro %}

{%- macro bigquery___count_num_rows_in_status() -%}
    count(distinct {{ dbt.concat(["dbt_audit_surrogate_key", "dbt_audit_pk_row_num"]) }}) over (partition by dbt_audit_row_status)
{% endmacro %}

{%- macro redshift___count_num_rows_in_status() -%}
    {#- Redshift doesn't support count(distinct) inside of window functions :( -#}
    {#- modified rows are the only ones that return two rows per PK/row num pairing, so just need to be halved -#}
    (count(*) over (partition by dbt_audit_row_status)) / case when dbt_audit_row_status = 'modified' then 2 else 1 end
{% endmacro %}