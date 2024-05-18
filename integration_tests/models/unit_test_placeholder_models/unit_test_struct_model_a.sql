{{ config(tags=['skip' if (target.type in ['redshift']) else 'runnable']) }}

select 1 as id, 'John Doe' as col1, object_construct('street', '123 Main St', 'city', 'Anytown', 'state', 'CA') as col2