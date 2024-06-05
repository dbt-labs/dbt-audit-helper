{#-
    Set generation is dispatched because it's possible to get performance optimisations 
    on some platforms, while keeping the post-processing standardised
    See https://infinitelambda.com/data-validation-refactoring-snowflake/ for an example and background
-#}

{% macro _generate_set_results(a_query, b_query, primary_key_columns, columns, event_time_props=None) %}
  {{ return(adapter.dispatch('_generate_set_results', 'audit_helper')(a_query, b_query, primary_key_columns, columns, event_time_props)) }}
{% endmacro %}

{% macro default___generate_set_results(a_query, b_query, primary_key_columns, columns, event_time_props) %}
    {% set joined_cols = columns | join(", ") %}

    a_base as (
        select 
            {{ joined_cols }}, 
            {{ audit_helper.generate_null_safe_surrogate_key(primary_key_columns) }} as dbt_audit_surrogate_key
        from ( {{-  a_query  -}} ) a_base_subq
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}
    ),

    b_base as (
        select 
            {{ joined_cols }}, 
            {{ audit_helper.generate_null_safe_surrogate_key(primary_key_columns) }} as dbt_audit_surrogate_key
        from ( {{-  b_query  -}} ) b_base_subq
        {% if event_time_props %}
            where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
            and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
        {% endif %}
    ),

    a as (
        select 
            *, 
            row_number() over (partition by dbt_audit_surrogate_key order by dbt_audit_surrogate_key) as dbt_audit_pk_row_num
        from a_base
    ),

    b as (
        select 
            *, 
            row_number() over (partition by dbt_audit_surrogate_key order by dbt_audit_surrogate_key) as dbt_audit_pk_row_num
        from b_base
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



{% macro bigquery___generate_set_results(a_query, b_query, primary_key_columns, columns, event_time_props) %}
    {% set joined_cols = columns | join(", ") %}
    subset_columns_a as (
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

    subset_columns_b as (
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

    a as (
        select
            *,
            farm_fingerprint(to_json_string(subset_columns_a)) as dbt_audit_row_hash
        from subset_columns_a
    ), 

    b as (
        select
            *,
            farm_fingerprint(to_json_string(subset_columns_b)) as dbt_audit_row_hash
        from subset_columns_b
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

{% macro snowflake___generate_set_results(a_query, b_query, primary_key_columns, columns, event_time_props) %}
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