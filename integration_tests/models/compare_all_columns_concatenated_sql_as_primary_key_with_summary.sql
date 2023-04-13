{% set a_relation=ref('data_compare_all_columns__market_of_choice_produce__concatenated_pk')%}

{% set b_relation=ref('data_compare_all_columns__albertsons_produce__concatenated_pk') %}

{{ audit_helper.compare_all_columns(
    a_relation=a_relation,
    b_relation=b_relation,
    primary_key="produce_category||id"
) }}
