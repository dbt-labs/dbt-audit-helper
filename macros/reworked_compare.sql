{% macro reworked_compare(a_query, b_query, primary_key_columns=[], columns=[], event_time=None, sample_limit=20) %}
    
    {% set joined_cols = columns | join(", ") %}

    {% if event_time %}
        {% set event_time_props = audit_helper.get_comparison_bounds(a_query, b_query, event_time) %}
    {% endif %}

    with 
    {#-
        Set generation is dispatched because it's possible to get performance optimisations 
        on some platforms, while keeping the post-processing standardised
        See https://infinitelambda.com/data-validation-refactoring-snowflake/ for an example and background
    -#}
    {{ audit_helper.generate_set_results(a_query, b_query, primary_key_columns, columns, event_time_props)}}
    
    ,

    all_records as (

        select
            *,
            true as in_a,
            true as in_b,
        from a_intersect_b

        union all

        select
            *,
            true as in_a,
            false as in_b
        from a_except_b

        union all

        select
            *,
            false as in_a,
            true as in_b
        from b_except_a

    ),


    classified as (
        
        select 
            *,
            case 
                when in_a and in_b then 'identical'
                when {{ dbt.bool_or('in_a') }} over (partition by dbt_audit_surrogate_key, dbt_audit_pk_row_num) 
                    and {{ dbt.bool_or('in_b') }} over (partition by dbt_audit_surrogate_key, dbt_audit_pk_row_num)
                then 'modified'
                when in_a then 'removed'
                when in_b then 'added'
            end as dbt_audit_row_status
        from all_records
        order by dbt_audit_surrogate_key, in_a desc, in_b desc

    ),

    final as (
        select 
            *,
            count(distinct dbt_audit_surrogate_key, dbt_audit_pk_row_num) over (partition by dbt_audit_row_status) as dbt_audit_num_rows_in_status,
            dense_rank() over (partition by dbt_audit_row_status order by dbt_audit_surrogate_key, dbt_audit_pk_row_num) as dbt_audit_sample_number
        from classified
    )

    select * from final
    {% if sample_limit %}
        where dbt_audit_sample_number <= {{ sample_limit }}
    {% endif %}
    order by dbt_audit_row_status, dbt_audit_sample_number

{% endmacro %}

{% macro generate_set_results(a_query, b_query, primary_key_columns, columns, event_time_props=None) %}
  {{ return(adapter.dispatch('generate_set_results', 'audit_helper')(a_query, b_query, primary_key_columns, columns, event_time_props)) }}
{% endmacro %}

{% macro default__generate_set_results(a_query, b_query, primary_key_columns, columns, event_time_props) %}
    {% set joined_cols = columns | join(", ") %}

    a as (
        select 
            {{ joined_cols }}, 
            {{ audit_helper.generate_null_safe_surrogate_key(primary_key_columns) }} as dbt_audit_surrogate_key,
            row_number() over (partition by dbt_audit_surrogate_key order by dbt_audit_surrogate_key ) as dbt_audit_pk_row_num
        from ( {{-  a_query  -}} )
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}
    ),

    b as (
        select 
            {{ joined_cols }}, 
            {{ audit_helper.generate_null_safe_surrogate_key(primary_key_columns) }} as dbt_audit_surrogate_key,
            row_number() over (partition by dbt_audit_surrogate_key order by dbt_audit_surrogate_key ) as dbt_audit_pk_row_num
        from ( {{-  b_query  -}} )
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}
    ),

    a_intersect_b as (

        select * from a
        {{ dbt.intersect() }}
        select * from b

    ),

    a_except_b as (

        select * from a
        {{ dbt.except() }}
        select * from b

    ),

    b_except_a as (

        select * from b
        {{ dbt.except() }}
        select * from a

    )
{% endmacro %}

{% macro snowflake__generate_set_results(a_query, b_query, primary_key_columns, columns, event_time_props) %}
    {% set joined_cols = columns | join(", ") %}
    a as (
        select 
            {{ joined_cols }}, 
            {{ audit_helper.generate_null_safe_surrogate_key(primary_key_columns) }} as dbt_audit_surrogate_key,
            row_number() over (partition by dbt_audit_surrogate_key order by dbt_audit_surrogate_key ) as dbt_audit_pk_row_num,
            hash({{ joined_cols }}, dbt_audit_pk_row_num) as dbt_audit_row_hash
        from ( {{-  a_query  -}} )
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}
    ),

    b as (
        select 
            {{ joined_cols }}, 
            {{ audit_helper.generate_null_safe_surrogate_key(primary_key_columns) }} as dbt_audit_surrogate_key,
            row_number() over (partition by dbt_audit_surrogate_key order by dbt_audit_surrogate_key ) as dbt_audit_pk_row_num,
            hash({{ joined_cols }}, dbt_audit_pk_row_num) as dbt_audit_row_hash
        from ( {{-  b_query  -}} )
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}
    ),

    a_intersect_b as (

        select * from a
        where a.dbt_audit_row_hash in (select b.dbt_audit_row_hash from b)

    ),

    a_except_b as (

        select * from a
        where a.dbt_audit_row_hash not in (select b.dbt_audit_row_hash from b)

    ),

    b_except_a as (

        select * from b
        where b.dbt_audit_row_hash not in (select a.dbt_audit_row_hash from a)

    )
{% endmacro %}