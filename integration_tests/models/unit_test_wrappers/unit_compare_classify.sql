{{ 
    audit_helper.compare_and_classify_query_results(
        "select * from " ~ ref('unit_test_model_a') ~ " where 1=1",
        "select * from " ~ ref('unit_test_model_b') ~ " where 1=1",
        primary_key_columns=var('primary_key_columns_var'),
        columns=var('columns_var'),
        event_time=var('event_time_var')
    ) 
}}