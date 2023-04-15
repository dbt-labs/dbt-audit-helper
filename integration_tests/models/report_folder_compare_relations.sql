-- depends_on: {{ ref('model_a') }}
-- depends_on: {{ ref('data_compare_relations__a_relation') }}
-- depends_on: {{ ref('model_b') }}
-- depends_on: {{ ref('data_compare_relations__b_relation') }}

{{ audit_helper.generate_audit_report_for_folder(
    model_folder_path = 'report', 
    identifiers = {'model_a': 'data_compare_relations__a_relation', 'model_b': 'data_compare_relations__b_relation'},
    compare_database = this.database, 
    compare_schema = this.schema
) }}