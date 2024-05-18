{{ config(tags=['skip' if (target.type in ['redshift']) else 'runnable']) }}

{{ 
    audit_helper.quick_are_queries_identical(
        "select * from " ~ ref('unit_test_model_a'),
        "select * from " ~ ref('unit_test_model_b'),
        columns=var('quick_are_queries_identical_cols'),
        event_time=var('quick_are_queries_identical_event_time')
    ) 
}}