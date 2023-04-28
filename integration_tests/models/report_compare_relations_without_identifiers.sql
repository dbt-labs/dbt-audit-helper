-- depends_on: {{ ref('model_a') }}

{{ audit_helper.generate_audit_report(
    model_list = ['model_a'],
    compare_database = this.database, 
    compare_schema = this.schema
) }}