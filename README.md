# dbt-audit-helper

Useful macros when performing data audits

# Contents
* [compare_relations](#compare_relations-source)
* [compare_queries](#compare_queries-source)
* [compare_column_values](#compare_column_values-source)
* [compare_relation_columns](#compare_relation_columns-source)
* [compare_all_columns](#compare_all_columns-source)
* [compare_column_values_verbose](#compare_column_values_verbose-source)

# Installation instructions
New to dbt packages? Read more about them [here](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/).
1. Include this package in your `packages.yml` file — check [here](https://hub.getdbt.com/dbt-labs/audit_helper/latest/) for the latest version number.
2. Run `dbt deps` to install the package.

# Macros
## compare_relations ([source](macros/compare_relations.sql))
This macro generates SQL that can be used to do a row-by-row validation of two
relations. It is largely based on the [equality](https://github.com/dbt-labs/dbt-utils#equality-source)
test in dbt-utils. By default, the generated query returns a summary of audit
results, like so:

| in_a  | in_b  | count | percent_of_total |
|-------|-------|------:|-----------------:|
| True  | True  | 6870  | 99.74            |
| True  | False | 9     | 0.13             |
| False | True  | 9     | 0.13             |

Setting the `summarize` argument to `false` lets you check which rows do not match between relations:

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
* `summarize` (optional): Allows you to switch between a summary or detailed view
  of the compared data. Accepts `true` or `false` values. Defaults to `true`.

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

Arguments:
* `summarize` (optional): Allows you to switch between a summary or detaied view
  of the compared data. Accepts `true` or `false` vaules. Defaults to `true`.

## compare_column_values ([source](macros/compare_column_values.sql))
This macro will return a query, that, when executed, compares a column across
two queries, and summarizes how many records match perfectly (note: a primary
key is required to match values across the two queries).

| match_status                | count  | percent_of_total |
|-----------------------------|-------:|-----------------:|
| ✅: perfect match            | 37,721 | 79.03            |
| ✅: both are null            | 5,789  | 12.13            |
| 🤷: missing from b          | 25     | 0.05             |
| 🤷: value is null in a only | 59     | 0.12             |
| 🤷: value is null in b only | 73     | 0.15             |
| 🙅: ‍values do not match    | 4,064  | 8.51             |

This macro is useful when:
* You've used the `compare_queries` macro (above) and found that a significant
number of your records don't match.
* So now you want to find which column is causing most of these discrepancies.

### Usage:
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

{% if execute %}
{% do audit_results.print_table() %}
{% endif %}
```

**Usage notes:**
* `primary_key` must be a unique key in both tables, otherwise the join won't
work as expected.


### Advanced usage - dbt Cloud:
The ``.print_table()`` function is not compatible with dbt Cloud so an adjustment needs to be made in order to print the results. Add the following code to a new macro file. To run the macro, execute `dbt run-operation print_audit_output()` in the command bar.
```
{% macro print_audit_output() %}
{%- set columns_to_compare=adapter.get_columns_in_relation(ref('fct_orders'))  -%}

{% set old_etl_relation_query %}
    select * from public.dim_product
{% endset %}

{% set new_etl_relation_query %}
    select * from {{ ref('fct_orders') }}
{% endset %}

{% if execute %}
    {% for column in columns_to_compare %}
        {{ log('Comparing column "' ~ column.name ~'"', info=True) }}
        {% set audit_query = audit_helper.compare_column_values(
                a_query=old_etl_relation_query,
                b_query=new_etl_relation_query,
                primary_key="order_id",
                column_to_compare=column.name
        ) %}

        {% set audit_results = run_query(audit_query) %}

        {% do log(audit_results.column_names, info=True) %}
            {% for row in audit_results.rows %}
                  {% do log(row.values(), info=True) %}
            {% endfor %}
    {% endfor %}
{% endif %}

{% endmacro %}
```


## compare_relation_columns ([source](macros/compare_relation_columns.sql))
This macro will return a query, that, when executed, compares the ordinal_position
and data_types of columns in two [Relations](https://docs.getdbt.com/docs/api-variable#section-relation).

| column_name | a_ordinal_position | b_ordinal_position | a_data_type       | b_data_type       |
|-------------|--------------------|--------------------|-------------------|-------------------|
| order_id    | 1                  | 1                  | integer           | integer           |
| customer_id | 2                  | 2                  | integer           | integer           |
| order_date  | 3                  | 3                  | timestamp         | date              |
| status      | 4                  | 5                  | character varying | character varying |
| amount      | 5                  | 4                  | bigint            | bigint            |


This is especially useful in two situations:
1. Comparing a new version of a relation with an old one, to make sure that the
structure is the same
2. Helping figure out why a `union` of two relations won't work (often because
the data types are different)

For example, in the above result set, we can see that `status` and `amount` have
switched order. Further, `order_date` is a timestamp in our "a" relation, whereas
it is a date in our "b" relation.

Note: For adapters other than BigQuery, Postgres, Redshift, and Snowflake, the ordinal_position is inferred based on the response from dbt Core's `adapter.get_columns_in_relation()`, as opposed to being loaded from the information schema.

```sql
{#- in dbt Develop -#}

{% set old_etl_relation=adapter.get_relation(
      database=target.database,
      schema="old_etl_schema",
      identifier="fct_orders"
) -%}

{% set dbt_relation=ref('fct_orders') %}

{{ audit_helper.compare_relation_columns(
    a_relation=old_etl_relation,
    b_relation=dbt_relation
) }}

```

## compare_all_columns ([source](macros/compare_all_columns.sql))
This macro is designed to be added to a dbt test suite as a custom test. A 
`compare_all_columns` test monitors changes data values when code is changed 
as part of a PR or during development. It sets up a test that will fail 
if any column values do not match. 

Users can configure what exactly constitutes a value match or failure. If 
there is a test failure, results can be inspected in the warehouse. The primary key 
and the column name can be included in the test output that gets written to the warehouse. 
This enables the user to join test results to relevant tables in your dev or prod schema to investigate the error.

### Usage:

_Note: this test should only be used on (and will only work on) models that have a primary key that is reliably `unique` and `not_null`. [Generic dbt tests](https://docs.getdbt.com/docs/building-a-dbt-project/tests#generic-tests) should be used to ensure the model being tested meets the requirements of `unique` and `not_null`._

To create a test for the `stg_customers` model, create a custom test 
in the `tests` subdirectory of your dbt project that looks like this:

```
{{ 
  audit_helper.compare_all_columns(
    a_relation=ref('stg_customers'), -- in a test, this ref will compile as your dev or PR schema.
    b_relation=api.Relation.create(database='dbt_db', schema='analytics_prod', identifier='stg_customers'), -- you can explicitly write a relation to select your production schema, or any other db/schema/table you'd like to use for comparison testing.
    exclude_columns=['updated_at'], 
    primary_key='id'
  ) 
}}
where not perfect_match
```
The `where not perfect_match` statement is an example of a filter you can apply to define what
constitutes a test failure. The test will fail if any rows don't meet the
requirement of a perfect match. Failures would include:

* If the primary key exists in both relations, but one model has a null value in a column.
* If a primary key is missing from one relation.
* If the primary key exists in both relations, but the value conflicts.

If you'd like the test to only fail when there are conflicting values, you could configure it like this:

```
{{ 
  audit_helper.compare_all_columns(
    a_relation=ref('stg_customers'), 
    b_relation=api.Relation.create(database='dbt_db', schema='analytics_prod', identifier='stg_customers'),
    primary_key='id'
  ) 
}}
where conflicting_values
```

#### Arguments:

* `a_relation` and `b_relation`: The [relations](https://docs.getdbt.com/reference#relation)
  you want to compare. Any two relations that have the same columns can be used. In the 
  example above, two different approaches to writing relations, using `ref` and 
  using `api.Relation.create`, are demonstrated. (When writing one-off code, it might make sense to
  hard-code a relation, like this: `analytics_prod.stg_customers`. A hard-coded relation
  is not recommended when building this macro into a CI cycle.)
* `exclude_columns` (optional): Any columns you wish to exclude from the
  validation.
* `primary_key`: The primary key of the model. Used to sort unmatched
  results for row-by-row validation.

If you want to create test results that include columns from the model itself 
for easier inspection, that can be written into the test:

```
{{ 
  audit_helper.compare_all_columns(
    a_relation=ref('stg_customers'),
    b_relation=api.Relation.create(database='dbt_db', schema='analytics_prod', identifier='stg_customers'), 
    exclude_columns=['updated_at'], 
    primary_key='id'
  ) 
}}
left join {{ ref('stg_customers') }} using(id)
```

This structure also allows for the test to group or filter by any attribute in the model or in 
the macro's output as part of the test, for example:

```
with base_test_cte as (
  {{ 
    audit_helper.compare_all_columns(
      a_relation=ref('stg_customers'),
      b_relation=api.Relation.create(database='dbt_db', schema='analytics_prod', identifier='stg_customers'), 
      exclude_columns=['updated_at'], 
      primary_key='id'
    ) 
  }}
  left join {{ ref('stg_customers') }} using(id)
  where conflicting_values
)
select
  status, -- assume there's a "status" column in stg_customers
  count(distinct case when conflicting_values then id end) as conflicting_values
from base_test_cte
group by 1
```

You can write a `compare_all_columns` test on individual table; and the test will be run 
as part of a full test suite run.

```
dbt test --select stg_customers
```

If you want to [store results in the warehouse for further analysis](https://docs.getdbt.com/docs/building-a-dbt-project/tests#storing-test-failures), add the `--store-failures`
flag.

```
dbt test --select stg_customers --store-failures
```

## compare_column_values_verbose ([source](macros/compare_column_values_verbose.sql))
This macro will return a query that, when executed, returns the same information as 
`compare_column_values`, but not summarized. `compare_column_values_verbose` enables `compare_all_columns` to give the user more flexibility around what will result in a test failure.


# To-do:
* Macro to check if two schemas contain the same relations
