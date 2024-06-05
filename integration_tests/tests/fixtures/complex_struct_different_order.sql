{% set json %}
    '{"emails":["john.doe@example.com","john.d@example.com"],"phones":[{"number":"987-654-3210","type":"work"}, {"number":"123-456-7890","type":"home"}]}'
{% endset %}

select 
    1 as id, 
    'John Doe' as col1, 
    {{ audit_helper_integration_tests._complex_json_function(json) }} as col2