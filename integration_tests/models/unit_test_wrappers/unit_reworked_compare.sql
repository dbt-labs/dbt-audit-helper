{{ 
    audit_helper.reworked_compare(
        "select * from " ~ ref('unit_test_model_a'),
        "select * from " ~ ref('unit_test_model_b'),
        primary_key_columns=var('reworked_compare__primary_key_columns'),
        columns=var('reworked_compare__columns'),
        event_time=var('reworked_compare__event_time')
    ) 
}}