{% set pk_cols = var('primary_key_columns_var') %}
{% set cols = var('columns_var') %}

{% if target.type == 'snowflake' and flags.WHICH == 'run' %}
    {% set pk_cols = pk_cols | map("upper") | list %}
    {% set cols = cols | map("upper") | list %}
{% endif %}

{{ 
    audit_helper.compare_which_query_columns_differ(
        a_query = "select * from " ~ ref('unit_test_model_a') ~ " where 1=1",
        b_query = "select * from " ~ ref('unit_test_model_b') ~ " where 1=1",
        primary_key_columns = pk_cols, 
        columns = cols,
        event_time = var('event_time_var')
    )
}}