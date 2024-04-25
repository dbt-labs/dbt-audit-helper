{% macro reworked_compare(a_query, b_query, primary_key_columns=[], columns=[], event_time=None, sample_limit=20) %}
    
    {% set joined_cols = columns | join(", ") %}
    {% set primary_key = primary_key_columns | join(", ") %}

    {% if event_time %}
        {% set event_time_props = audit_helper.get_comparison_bounds(a_query, b_query, event_time) %}
    {% endif %}

    with 

    {{ audit_helper.generate_set_results(a_query, b_query, columns, event_time_props)}}
    
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
                when {{ dbt.bool_or('in_a') }} over (partition by {{ primary_key }}) 
                    and {{ dbt.bool_or('in_b') }} over (partition by {{ primary_key }})
                then 'modified'
                when in_a then 'removed'
                when in_b then 'added'
            end as status
        from all_records
        order by {{ primary_key }}, in_a desc, in_b desc

    ),

    final as (
        select 
            *,
            count(distinct {{ primary_key }}) over (partition by status) as num_in_status,
            dense_rank() over (partition by status order by {{ primary_key }}) as sample_number
        from classified
    )

    select * from final
    {% if sample_limit %}
        where sample_number <= {{ sample_limit }}
    {% endif %}
    order by status, sample_number

{% endmacro %}

{% macro generate_set_results(a_query, b_query, columns, event_time_props=None) %}
  {{ return(adapter.dispatch('generate_set_results', 'audit_helper')(a_query, b_query, columns, event_time_props)) }}
{% endmacro %}

{% macro default__generate_set_results(a_query, b_query, columns, event_time_props) %}
    {% set joined_cols = columns | join(", ") %}

    a as (
        select {{ joined_cols }}
        from ( {{-  a_query  -}} )
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}
    ),

    b as (
        select {{ joined_cols }}
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

{% macro snowflake__generate_set_results(a_query, b_query, columns, event_time_props) %}
    {% set joined_cols = columns | join(", ") %}
    a as (
        select 
            {{ joined_cols }},
            hash({{ joined_cols }}) as dbt_compare_row_hash
        from ( {{-  a_query  -}} )
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}
    ),

    b as (
        select 
            {{ joined_cols }},
            hash({{ joined_cols }}) as dbt_compare_row_hash
        from ( {{-  b_query  -}} )
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}
    ),

    a_intersect_b as (

        select * from a
        where a.dbt_compare_row_hash in (select b.dbt_compare_row_hash from b)

    ),

    a_except_b as (

        select * from a
        where a.dbt_compare_row_hash not in (select b.dbt_compare_row_hash from b)

    ),

    b_except_a as (

        select * from b
        where b.dbt_compare_row_hash not in (select a.dbt_compare_row_hash from a)

    )
{% endmacro %}