{{ config(tags=['skip' if (target.type in ['postgres']) else 'runnable']) }}

select 
    1 as id, 
    'John Doe' as col1, 
    {{ audit_helper_integration_tests._basic_json_function() -}}('street', '123 Main St', 'city', 'Anytown', 'state', 'CA') as col2
