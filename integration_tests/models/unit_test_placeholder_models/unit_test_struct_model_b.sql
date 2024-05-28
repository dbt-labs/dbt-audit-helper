{{ config(tags=['skip' if (target.type in ['postgres']) else 'runnable']) }}

{% if target.name != 'redshift' %}

select 
    1 as id, 
    'John Doe' as col1, 
    {{ audit_helper_integration_tests._basic_json_function() -}}('street', '123 Main St', 'city', 'Anytown', 'state', 'CA') as col2

{% else %}

select 
  1 AS id, 
  'John Doe' AS col1, 
  json_parse('{"street": "123 Main St", "city": "Anytown", "state": "CA"}') AS col2
{% endif %}