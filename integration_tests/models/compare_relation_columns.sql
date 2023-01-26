
with audit_helper_results as (
    {{ audit_helper.compare_relation_columns(
        a_relation=ref('data_compare_relation_columns_a'),
        b_relation=ref('data_compare_relation_columns_b')
    ) }}
)

select 
    --These need to be cast, otherwise they are technically typed as "sql_identifier" or "cardinal_number" on Redshift
    {{ "lower(" if target.type == 'snowflake' }} cast(column_name as {{ dbt.type_string() }}) {{ ")" if target.type == 'snowflake' }} as column_name, 
    cast(a_ordinal_position as {{ dbt.type_int() }}) as a_ordinal_position,
    cast(b_ordinal_position as {{ dbt.type_int() }}) as b_ordinal_position,
    --not checking the specific datatypes, as long as they match/don't match as expected then that's still checking the audit behaviour
    has_ordinal_position_match,
    has_data_type_match
from audit_helper_results