{% set a_relation=ref('data_compare_all_columns__a_relation')%}

{% set b_relation=ref('data_compare_all_columns__b_relation') %}

{{ audit_helper.compare_all_columns(
    a_relation=a_relation,
    b_relation=b_relation,
    primary_key="col_a",
    summarize=false
) }}