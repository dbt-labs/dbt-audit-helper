
with audit_helper_results as (
    {{ audit_helper.compare_relation_columns(
        a_relation=ref('data_compare_relation_columns_a'),
        b_relation=ref('data_compare_relation_columns_b')
    ) }}
)

select 
    cast(column_name as dbt.type_string()) as column_name, --otherwise it is technically a "sql_identifier" type on Redshift
    a_ordinal_position,
    b_ordinal_position,
    --not checking the specific datatypes, as long as they match/don't match as expected then that's still checking the audit behaviour
    has_ordinal_position_match,
    has_data_type_match
from audit_helper_results