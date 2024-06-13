/*
The idea here is that if the event_time is set, we will only compare records enclosed in both models.
This improves performance and allows us to compare apples to apples, instead of detecting millions/billions
of "deletions" identified due to prod having all data while CI only has a few days' worth.

In the diagram below, the thatched section is the comparison bounds. You can think of it as
                                                         
         greatest(model_a.min_value, model_b.min_value)  
            least(model_a.max_value, model_b.max_value)  
                                                         
                 ┌────────────────────────────┐          
  a min_value    │                a max_value │        
    └──► ┌───────┼────────────────────┐ ◄───┘ │        
         │       │┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼│       │        
model_a  │       │┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼│       │ model_b
         │       │┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼┼│       │        
         └───────┼────────────────────┘       │        
            ┌──► └────────────────────────────┘ ◄────┐ 
           b min_value                      b max_value 
*/
{% macro _get_comparison_bounds(a_query, b_query, event_time) %}
    {% set min_max_queries %}
        with min_maxes as (
            select min({{ event_time }}) as min_event_time, max({{ event_time }}) as max_event_time
            from ({{ a_query }}) a_subq
            union all 
            select min({{ event_time }}) as min_event_time, max({{ event_time }}) as max_event_time
            from ({{ b_query }}) b_subq
        )
        select max(min_event_time) as min_event_time, min(max_event_time) as max_event_time
        from min_maxes
    {% endset %}

    {% set query_response = dbt_utils.get_query_results_as_dict(min_max_queries) %}
    
    {% set event_time_props = {"event_time": event_time} %}
    
    {# query_response.keys() are only `min_event_time` and `max_event_time`, but they have indeterminate capitalisation #}
    {# hence the dynamic approach for what is otherwise just two well-known values #}
    {% for k in query_response.keys() %}
        {% do event_time_props.update({k | lower: query_response[k][0]}) %}
    {% endfor %}
    
    {% do return(event_time_props) %}
{% endmacro %}

{% macro event_time_filter(event_time_props) %}
    {% if event_time_props %}
        where {{ event_time_props["event_time"] }} >= '{{ event_time_props["min_event_time"] }}'
        and {{ event_time_props["event_time"] }} <= '{{ event_time_props["max_event_time"] }}'
    {% endif %}
{% endmacro %}