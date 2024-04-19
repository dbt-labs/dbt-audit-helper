{% macro get_comparison_bounds(a_relation, b_relation, event_time) %}
    {% set min_max_queries %}
        with min_maxes as (
            select min({{ event_time }}) as min_event_time, max({{ event_time }}) as max_event_time
            from {{ a_relation }}
            union all 
            select min({{ event_time }}) as min_event_time, max({{ event_time }}) as max_event_time
            from {{ b_relation }}
        )
        select max(min_event_time) as "min_event_time", min(max_event_time) as "max_event_time"
        from min_maxes
    {% endset %}

    {% set query_response = dbt_utils.get_query_results_as_dict(min_max_queries) %}
    
    {% set min_max_event_time_results = {} %}
    {% for k in query_response.keys() %}
        {% do min_max_event_time_results.update({k: query_response[k][0]}) %}
    {% endfor %}
    
    {% do return(min_max_event_time_results) %}
{% endmacro %}