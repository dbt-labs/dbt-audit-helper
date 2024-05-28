
{{ 
    audit_helper.compare_queries(
        "select * from " ~ ref('unit_test_model_a') ~ " where 1=1",
        "select * from " ~ ref('unit_test_model_b') ~ " where 1=1",
        summarize = var('compare_queries_summarize')
    ) 
}}