{{ 
    audit_helper.reworked_compare(
        "select * from " ~ ref('unit_test_struct_model_a') ~ " where 1=1",
        "select * from " ~ ref('unit_test_struct_model_b') ~ " where 1=1",
        primary_key_columns=var('reworked_compare__primary_key_columns'),
        columns=var('reworked_compare__columns'),
        event_time=var('reworked_compare__event_time')
    ) 
}}