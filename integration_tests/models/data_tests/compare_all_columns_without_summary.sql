{% set a_relation=ref('data_compare_all_columns__market_of_choice_produce')%}

{% set b_relation=ref('data_compare_all_columns__albertsons_produce') %}

{{ audit_helper.compare_all_columns(
    a_relation=a_relation,
    b_relation=b_relation,
    primary_key="id",
    summarize=false
) }}
