{% set results = 
    audit_helper._ensure_all_pks_are_in_column_set(
        primary_key_columns=var('primary_key_columns_var', ['a_column_with_a_large_unwieldy_name']),
        columns=var('columns_var', ['b_column_with_a_large_unwieldy_name']),
    )
%}

{% if (var('primary_key_columns_var') | length == 0) and (var('columns_var') | length == 0) %}
-- need to still provide a table shape
select 'abcdefabcdef' as col, 1 as row_index
limit 0
{% endif %}

{% for result in results %}
    select '{{ result }}' as col, {{ loop.index }} as row_index
    {% if not loop.last %}
    union all 
    {% endif %}
{% endfor %}