{{ 
    audit_helper.reworked_compare(
        "select * from " ~ ref('unit_test_model_a'),
        "select * from " ~ ref('unit_test_model_b_more_cols'),
        primary_key_columns=['id'],
        columns=var('reworked_compare__columns'),
        event_time=var('reworked_compare__event_time')
    ) 
}}