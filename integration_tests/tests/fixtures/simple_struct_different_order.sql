{% if target.name != 'redshift' %}

select 
    1 as id, 
    'John Doe' as col1, 
    {{ audit_helper_integration_tests._basic_json_function() -}}( 'state', 'CA', 'street', '123 Main St', 'city', 'Anytown') as col2

{% else %}

select 
  1 AS id, 
  'John Doe' AS col1, 
  json_parse('{"state": "CA", "street": "123 Main St", "city": "Anytown"}') AS col2
{% endif %}