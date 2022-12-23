/*
The [dbt Audit Helper package](https://github.com/dbt-labs/dbt-audit-helper#compare_column_values-source)
includes a macro called `compare_column_values` that takes two queries and compares the values in a specific column.
In the documentation, it gives an example of how to loop through all the columns in the two tables, _however_,
this pattern does work because it relies on `agate.Table.print_table`, which outputs to `stdout`,
which in turn is not supported in dbt Cloud (logs are sent to `stderr`).

This macro wraps the `compare_column_values` macro to
(1) Detect whether the dbt environment is dbt-core or dbt Cloud and output results to the log accordingly.
(2) If executed in dbt Cloud, requires `dbt run-operation` to output the results to the logs.
(3) Assumes the primary use case is comparing an old table in production to a modified version of the same table in the user's Develop environment.

arguments:
old_table: includes full namespace e.g. my_production_database.my_production_schema.my_production_table
new_table_ref: model name in your Develop environment e.g. my_model_name -- not a ref object, just the name
primary_key: must be unique in both for join. See https://github.com/dbt-labs/dbt-audit-helper#compare_column_values-source

usage:
dbt run-operation compare_table_values --args '{"old_table": "my_production_database.my_production_schema.my_production_table", "new_table_ref": "my_production_table", "primary_key": "my_primary_key"}'
*/

{% macro compare_table_values(old_table, new_table_ref, primary_key) %}
{%- set columns_to_compare=adapter.get_columns_in_relation(ref(new_table_ref))  -%}

{% set old_etl_relation_query %}
    select * from {{ old_table }}
{% endset %}

{% set new_etl_relation_query %}
    select * from {{ ref(new_table_ref) }}
{% endset %}

{% if execute %}
    {% for column in columns_to_compare %}
        {{ log('Comparing column "' ~ column.name ~'"', info=True) }}
        {% set audit_query = audit_helper.compare_column_values(
                a_query=old_etl_relation_query,
                b_query=new_etl_relation_query,
                primary_key=primary_key,
                column_to_compare=column.name
        ) %}

        {% set audit_results = run_query(audit_query) %}

        {% cloud_safe_print_table(audit_results) %}
    {% endfor %}
{% endif %}

{% endmacro %}

{% macro is_cloud() %}
    {# In the dbt Cloud context, `DBT_ENV` will always resolve to "prod" #}
    {% set is_cloud = env_var("DBT_ENV", "core") == "prod" %}
    {{ return(is_cloud) }}
{% endmacro %}

{% macro cloud_safe_print_table(audit_results) %}
{% if execute %}
    {% if is_cloud() %}
        {% do log(audit_results.column_names, info=True) %}
        {% for row in audit_results.rows %}
            {% do log(row.values(), info=True) %}
        {% endfor %}
    {% else %}
        {% do audit_results.print_table() %}
        {{ log("", info=True) }}
    {% endif %}
{% endif %}
{% endmacro %}