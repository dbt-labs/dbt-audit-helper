{% macro _classify_audit_row_status() %}
    {{ return(adapter.dispatch('_classify_audit_row_status', 'audit_helper')()) }}
{% endmacro %}

{%- macro default___classify_audit_row_status() -%}
    case 
        when max(dbt_audit_pk_row_num) over (partition by dbt_audit_surrogate_key) > 1 then 'nonunique_pk'
        when in_a and in_b then 'identical'
        when {{ dbt.bool_or('in_a') }} over (partition by dbt_audit_surrogate_key, dbt_audit_pk_row_num) 
            and {{ dbt.bool_or('in_b') }} over (partition by dbt_audit_surrogate_key, dbt_audit_pk_row_num)
            then 'modified'
        when in_a then 'removed'
        when in_b then 'added'
    end
{% endmacro %}


{%- macro redshift___classify_audit_row_status() -%}
    {#- Redshift doesn't support bitwise operations (e.g. bool_or) inside of a window function :( -#}
    case 
        when max(dbt_audit_pk_row_num) over (partition by dbt_audit_surrogate_key) > 1 then 'nonunique_pk'
        when in_a and in_b then 'identical'
        when max(case when in_a then 1 else 0 end) over (partition by dbt_audit_surrogate_key, dbt_audit_pk_row_num) = 1
            and max(case when in_b then 1 else 0 end) over (partition by dbt_audit_surrogate_key, dbt_audit_pk_row_num) = 1
            then 'modified'
        when in_a then 'removed'
        when in_b then 'added'
    end{% endmacro %}