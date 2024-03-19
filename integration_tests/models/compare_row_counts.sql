{% set a_relation=ref('data_compare_relations__a_relation')%}

{% set b_relation=ref('data_compare_relations__b_relation') %}

select 
    case
        when relation_name = '{{ a_relation }}'
            then 'a'
        else 'b'
    end as relation_name, 
    total_records

from (

    {{ audit_helper.compare_row_counts(
        a_relation=a_relation,
        b_relation=b_relation
    ) }}

) as base_query 