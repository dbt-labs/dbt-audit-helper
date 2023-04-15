-- depends_on: {{ ref('model_a') }}
-- depends_on: {{ ref('data_compare_relations__a_relation') }}
-- depends_on: {{ ref('model_b') }}
-- depends_on: {{ ref('data_compare_relations__b_relation') }}

{{ audit_helper.generate_audit_report(
    model_list = ['model_a', 'model_b'],
    identifiers = {'model_a': 'data_compare_relations__a_relation', 'model_b': 'data_compare_relations__b_relation'},
    exclude_columns = {'model_a': ['col_a']},
    compare_database = this.database, 
    compare_schema = this.schema
) }}