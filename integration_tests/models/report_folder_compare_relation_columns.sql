-- depends_on: {{ ref('model_a') }}
-- depends_on: {{ ref('data_compare_relations__a_relation') }}
-- depends_on: {{ ref('model_b') }}
-- depends_on: {{ ref('data_compare_relations__b_relation') }}

with audit_helper_results as (    
    {{ audit_helper.generate_audit_report_for_folder(
        model_folder_path = 'report', 
        identifiers = {'model_a': 'data_compare_relations__a_relation', 'model_b': 'data_compare_relations__b_relation'},
        compare_database = this.database, 
        compare_schema = this.schema,
        audit_helper_macro_name = 'compare_relation_columns'
    ) }}
)

select 
    audit_model,
    --These need to be cast, otherwise they are technically typed as "sql_identifier" or "cardinal_number" on Redshift
    cast(column_name as {{ dbt.type_string() }}) as column_name, 
    cast(a_ordinal_position as {{ dbt.type_int() }}) as a_ordinal_position,
    cast(b_ordinal_position as {{ dbt.type_int() }}) as b_ordinal_position,
    --not checking the specific datatypes, as long as they match/don't match as expected then that's still checking the audit behaviour
    has_ordinal_position_match,
    has_data_type_match
from audit_helper_results