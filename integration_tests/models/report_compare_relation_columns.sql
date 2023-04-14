{{ audit_helper.generate_audit_report(
    model_folder_path = 'report', 
    identifiers = {'model_a': 'data_compare_relations__a_relation', 'model_b': 'data_compare_relations__b_relation'},
    compare_database = this.database, 
    compare_schema = this.schema,
    audit_helper_macro_name = 'compare_relation_columns'
) }}