{% set a_relation=ref('data_compare_which_columns_differ_a')%}

{% set b_relation=ref('data_compare_which_columns_differ_b') %}


select 
    lower(column_name) as column_name,
    has_difference
from (

    {{ audit_helper.compare_which_relation_columns_differ(
        a_relation=a_relation,
        b_relation=b_relation,
        primary_key=['id'],
        columns=['id','value_changes','becomes_not_null','does_not_change']
    ) }}

) as macro_output