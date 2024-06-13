{% set json %}
'{"emails":["john.smith@example.com","john.s@example.com"],"phones":[{"number":"123-456-7890","type":"home"},{"number":"987-654-3210","type":"work"}]}'
{% endset %}

select 
    1 as id, 
    'John Doe' as col1, 
    {{ audit_helper_integration_tests._complex_json_function(json) }} as col2