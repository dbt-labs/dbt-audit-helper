{% if target.name != 'redshift' %}

select 
    1 as id, 
    'John Doe' as col1, 
    {{ audit_helper_integration_tests._basic_json_function() -}}('street', '123 Main St', 'state', 'CA') as col2

{% else %}

select 
  1 AS id, 
  'John Doe' AS col1, 
  json_parse('{"street": "123 Main St", "state": "CA"}') AS col2
{% endif %}