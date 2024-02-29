{% set a_relation=ref('data_detect_column_changes_a')%}

{% set b_relation=ref('data_detect_column_changes_b') %}

{{ audit_helper.detect_column_changes(
    a_relation=a_relation,
    b_relation=b_relation,
    primary_key="id"
) }}
