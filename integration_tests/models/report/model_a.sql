select 
    col_a,
    col_b
from {{ ref('data_compare_relations__a_relation') }}
where col_a <> 1