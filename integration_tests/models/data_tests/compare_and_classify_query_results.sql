-- this has no tests, it's just making sure that the introspecive queries for event_time actually run

{{
    audit_helper.compare_and_classify_query_results(
        a_query="select * from " ~ ref('unit_test_model_a') ~ " where 1=1",
        b_query="select * from " ~ ref('unit_test_model_b') ~ " where 1=1",
        primary_key_columns=['id'],
        columns=['id', 'col1', 'col2'],
        event_time='created_at'
    )
}}