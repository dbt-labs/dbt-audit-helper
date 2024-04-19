
{{ 
    audit_helper.compare_queries(
        "select * from " ~ ref('unit_test_model_a'),
        "select * from " ~ ref('unit_test_model_b'),
        summarize = var('compare_queries_summarize')
    ) 
}}