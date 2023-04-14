{{ audit_helper.generate_audit_report(
    model_list = ['model_a', 'model_b'],
    compare_database = this.database, 
    compare_schema = this.schema
) }}