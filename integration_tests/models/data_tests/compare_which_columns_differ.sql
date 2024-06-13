{% set a_relation=ref('data_compare_which_columns_differ_a')%}

{% set b_relation=ref('data_compare_which_columns_differ_b') %}

-- lowercase for CI

select 
    lower(column_name) as column_name,
    has_difference
from (

    {{ audit_helper.compare_which_relation_columns_differ(
        a_relation=a_relation,
        b_relation=b_relation,
        primary_key_columns=["id"]
    ) }}
) as macro_output
