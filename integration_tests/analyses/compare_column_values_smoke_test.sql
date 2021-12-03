{% set a_query %}
    select * from {{ ref('data_compare_relations__a_relation') }}
{% endset %}

{% set audit_query = audit_helper.compare_column_values(
    a_query=a_query,
    b_query=a_query,
    primary_key="col_a",
    column_to_compare="col_b"
) %}

{{ audit_query }}

{% if execute %}

{% set audit_results = run_query(audit_query) %}

{% do audit_results.print_table() %}

{% endif %}
