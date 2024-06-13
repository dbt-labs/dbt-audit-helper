{{ 
    audit_helper.compare_which_query_columns_differ(
        a_query = "select * from " ~ ref('unit_test_model_a') ~ " where 1=1",
        b_query = "select * from " ~ ref('unit_test_model_b') ~ " where 1=1",
        primary_key_columns = var('primary_key_columns_var'), 
        columns = var('columns_var'),
        event_time = var('event_time_var')
    )
}}