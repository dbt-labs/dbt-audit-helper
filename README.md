# dbt-audit-helper

Useful macros when performing data audits

# Macros
## compare_relations ([source](macros/compare_relations.sql))
This macro generates SQL that can be used to do a row-by-row validation of two
relations. It is largely based on the [equality](https://github.com/fishtown-analytics/dbt-utils#equality-source)
test in dbt-utils. By default, the generated query returns a summary of audit
results, like so:

| in_a  | in_b  | count |
|-------|-------|-------|
| True  | True  | 6870  |
| True  | False | 9     |
| False | True  | 9     |

The generated SQL also contains commented-out SQL that you can use to check
the rows that do not match perfectly:

| order_id | order_date | status    | in_a  | in_b  |
|----------|------------|-----------|-------|-------|
| 1        | 2018-01-01 | completed | True  | False |
| 1        | 2018-01-01 | returned  | False | True  |
| 2        | 2018-01-02 | completed | True  | False |
| 2        | 2018-01-02 | returned  | False | True  |


This query is particularly useful when you want to check that a refactored model,
or a model that you are moving over from a legacy system, match up.

Usage:
The query is best used in dbt Develop so you can interactively check results
```sql
{# in dbt Develop #}

{% set old_etl_relation=adapter.get_relation(
      database=target.database,
      schema="old_etl_schema",
      identifier="fct_orders"
) -%}

{% set dbt_relation=ref('fct_orders') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["loaded_at"],
    primary_key="order_id"
) }}


```
Arguments:
* `a_relation` and `b_relation`: The [relations](https://docs.getdbt.com/reference#relation)
  you want to compare.
* `exclude_columns` (optional): Any columns you wish to exclude from the
  validation.
* `primary_key` (optional): The primary key of the model. Used to sort unmatched
  results for row-by-row validation.

# To-do:
* Macro to check if two models have the same structure
* Macro to check if two schemas contain the same relations
* Extend `compare_relations` macro to handle edge cases
