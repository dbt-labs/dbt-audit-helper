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

## compare_queries ([source](macros/compare_queries.sql))
Super similar to `compare_relations`, except it takes two select statements. This macro is useful when:
* You need to filter out records from one of the relations.
* You need to rename or recast some columns to get them to match up.
* You only want to compare a small number of columns, so it's easier write the columns you want to compare, rather than the columns you want to exclude.

```sql
{# in dbt Develop #}

{% set old_fct_orders_query %}
  select
    id as order_id,
    amount,
    customer_id
  from old_etl_schema.fct_orders
{% endset %}

{% set new_fct_orders_query %}
  select
    order_id,
    amount,
    customer_id
  from {{ ref('fct_orders') }}
{% endset %}

{{ audit_helper.compare_queries(
    a_query=old_fct_orders_query,
    b_query=new_fct_orders_query,
    primary_key="order_id"
) }}


```

## compare_column_values
This macro is useful when:
* You've used the `compare_queries` macro (above) and found that a significant
number of your records don't match.
* So now you want to check if a particular column is problematic.
```
{# in dbt Develop #}


{% set old_etl_relation_query %}
    select * from public.dim_product
    where is_latest
{% endset %}

{% set new_etl_relation_query %}
    select * from {{ ref('dim_product') }}
{% endset %}

{% set audit_query = audit_helper.compare_column_values(
    a_query=old_etl_relation_query,
    b_query=new_etl_relation_query,
    primary_key="product_id",
    column_to_compare="status"
) %}

{% set audit_results = run_query(audit_query) %}

{% do audit_results.print_table() %}
```
This will give you an output like:
```
Comparing column "status"
| match_status            |  count |
| ----------------------- | ------ |
| ✅: perfect match       | 37,721 |
| 🤷: missing from b      |     25 |
| 🙅: ‍values do not match |  4,064 |
```

Usage notes:
* `primary_key` must be a unique key in both tables, otherwise the join won't
work as expected.


### Advanced usage
Got a wide table, and want to iterate through all the columns? Try something
like this:
```
{%- set columns_to_compare=adapter.get_columns_in_relation(ref('dim_product'))  -%}

{% set old_etl_relation_query %}
    select * from public.dim_product
    where is_latest
{% endset %}

{% set new_etl_relation_query %}
    select * from {{ ref('dim_product') }}
{% endset %}

{% if execute %}
    {% for column in columns_to_compare %}
        {{ log('Comparing column "' ~ column.name ~'"', info=True) }}

        {% set audit_query = audit_helper.compare_column_values(
            a_query=old_etl_relation_query,
            b_query=new_etl_relation_query,
            primary_key="product_id",
            column_to_compare=column.name
        ) %}

        {% set audit_results = run_query(audit_query) %}
        {% do audit_results.print_table() %}
        {{ log("", info=True) }}

    {% endfor %}
{% endif %}
```

This will give you an output like:
```
| match_status      |  count |
| ----------------- | ------ |
| ✅: perfect match  | 41,785 |
| 🤷: missing from b |     26 |

Comparing column "name"
| match_status         |  count |
| -------------------- | ------ |
| ✅: perfect match     | 41,573 |
| 🤷: missing from b    |     26 |
| 🙅: ‍values do not... |    212 |

Comparing column "cost_per_unit"
| match_status         |  count |
| -------------------- | ------ |
| ✅: perfect match     | 27,449 |
| ✅: both are null     | 14,294 |
| 🤷: missing from b    |     23 |
| 🤷: exists, but nu... |      1 |
| 🤷: exists, but nu... |     40 |
| 🙅: ‍values do not... |      4 |

Comparing column "status"
| match_status         |  count |
| -------------------- | ------ |
| ✅: perfect match     | 37,715 |
| 🤷: missing from b    |     26 |
| 🙅: ‍values do not... |  4,070 |
```

# To-do:
* Macro to check if two models have the same structure
* Macro to check if two schemas contain the same relations
