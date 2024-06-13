{% macro compare_and_classify_query_results(a_query, b_query, primary_key_columns=[], columns=[], event_time=None, sample_limit=20) %}
    
    {% set columns = audit_helper._ensure_all_pks_are_in_column_set(primary_key_columns, columns) %}
    {% set joined_cols = columns | join(", ") %}

    {% if event_time %}
        {% set event_time_props = audit_helper._get_comparison_bounds(a_query, b_query, event_time) %}
    {% endif %}

    with 

    {{ audit_helper._generate_set_results(a_query, b_query, primary_key_columns, columns, event_time_props)}}
    
    ,

    all_records as (

        select
            *,
            true as dbt_audit_in_a,
            true as dbt_audit_in_b
        from a_intersect_b

        union all

        select
            *,
            true as dbt_audit_in_a,
            false as dbt_audit_in_b
        from a_except_b

        union all

        select
            *,
            false as dbt_audit_in_a,
            true as dbt_audit_in_b
        from b_except_a

    ),

    classified as (
        select 
            *,
            {{ audit_helper._classify_audit_row_status() }} as dbt_audit_row_status
        from all_records
    ),

    final as (
        select 
            *,
            {{ audit_helper._count_num_rows_in_status() }} as dbt_audit_num_rows_in_status,
            -- using dense_rank so that modified rows (which have a full row for both the left and right side) both get picked up in the sample. 
            -- For every other type this is equivalent to a row_number()
            dense_rank() over (partition by dbt_audit_row_status order by dbt_audit_surrogate_key, dbt_audit_pk_row_num) as dbt_audit_sample_number
        from classified
    )

    select * from final
    {% if sample_limit %}
        where dbt_audit_sample_number <= {{ sample_limit }}
    {% endif %}
    order by dbt_audit_row_status, dbt_audit_sample_number

{% endmacro %}