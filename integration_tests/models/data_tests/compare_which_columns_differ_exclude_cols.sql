{% set a_relation=ref('data_compare_which_columns_differ_a')%}

{% set b_relation=ref('data_compare_which_columns_differ_b') %}

{% set pk_cols = ['id'] %}
{% set cols = ['id','value_changes','becomes_not_null','does_not_change'] %}

{% if target.type == 'snowflake' %}
    {% set pk_cols = pk_cols | map("upper") | list %}
    {% set cols = cols | map("upper") | list %}
{% endif %}

select 
    lower(column_name) as column_name,
    has_difference
from (

    {{ audit_helper.compare_which_relation_columns_differ(
        a_relation=a_relation,
        b_relation=b_relation,
        primary_key_columns=pk_cols,
        columns=cols
    ) }}

) as macro_output