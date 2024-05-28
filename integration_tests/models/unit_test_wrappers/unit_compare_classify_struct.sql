{{ 
    audit_helper.compare_and_classify_query_results(
        "select * from " ~ ref('unit_test_struct_model_a') ~ " where 1=1",
        "select * from " ~ ref('unit_test_struct_model_b') ~ " where 1=1",
        primary_key_columns=var('compare_classify__primary_key_columns'),
        columns=var('compare_classify__columns'),
        event_time=var('compare_classify__event_time')
    ) 
}}