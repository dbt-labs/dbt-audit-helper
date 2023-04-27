{{ 
    config(
        alias='b_model'
    ) 
}}

select 
    col_a,
    col_b
from {{ ref('data_compare_relations__b_relation') }}