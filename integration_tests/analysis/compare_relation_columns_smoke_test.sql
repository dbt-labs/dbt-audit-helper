{% set a_relation=ref('data_compare_relations__a_relation') %}

{% set compare_relation_columns_sql = audit_helper.compare_relation_columns(
    a_relation,
    a_relation
) %}

{{ compare_relation_columns_sql }}

{% if execute %}

{% set results = run_query(compare_relation_columns_sql) %}
{% do results.print_table()  %}

{% endif %}
