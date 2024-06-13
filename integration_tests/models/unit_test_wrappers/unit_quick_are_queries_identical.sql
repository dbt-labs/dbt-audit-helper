{{ config(tags=['skip' if (target.type in ['redshift', 'postgres', 'databricks']) else 'runnable']) }}

{{ 
    audit_helper.quick_are_queries_identical(
        "select * from " ~ ref('unit_test_model_a') ~ " where 1=1",
        "select * from " ~ ref('unit_test_model_b') ~ " where 1=1",
        columns=var('quick_are_queries_identical_cols'),
        event_time=var('event_time_var')
    ) 
}}  