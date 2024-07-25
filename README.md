# dbt-audit-helper

Useful macros when performing data audits

## Contents

- [Installation instructions](#installation-instructions)
- [Compare Data Outputs](#compare-data-outputs)
  - Compare and classify:  
    - [compare_and_classify_query_results](#compare_and_classify_query_results-source)
    - [compare_and_classify_relation_rows](#compare_and_classify_relation_rows-source)
  - Quick identical check:
    - [quick_are_queries_identical](#quick_are_queries_identical-source)
    - [quick_are_relations_identical](#quick_are_relations_identical-source)
  - [compare\_row\_counts](#compare_row_counts-source)
- [Compare Columns](#compare-columns)
  - [compare\_column\_values](#compare_column_values-source)
  - [compare\_all\_columns](#compare_all_columns-source)
  - Compare which columns differ:
    - [compare\_which\_query\_columns\_differ](#compare_which_query_columns_differ-source)
    - [compare\_which\_relation\_columns\_differ](#compare_which_relation_columns_differ-source)
  - [compare\_relation\_columns](#compare_relation_columns-source)
- [Advanced Usage](#advanced-usage)
  - [Print Output To Logs](#print-output-to-logs)
  - [Use Output For Custom Singular Test](#use-output-for-custom-singular-test)
- [Legacy Macros](#legacy-macros)
  - [compare\_queries](#compare_queries-source)
  - [compare\_relations](#compare_relations-source)
- [Internal Macros](#internal-macros)

## Installation instructions

New to dbt packages? Read more about them [here](https://docs.getdbt.com/docs/building-a-dbt-project/package-management/).

1. Include this package in your `packages.yml` file — check [here](https://hub.getdbt.com/dbt-labs/audit_helper/latest/) for the latest version number.
2. Run `dbt deps` to install the package.

## Compare Data Outputs

### compare_and_classify_query_results ([source](macros/compare_and_classify_query_results.sql))

Generates a row-by-row comparison of two queries, as well as summary stats of added, removed, identical and modified records. This prevents you from having to query your comparison tables multiple times to get raw data and summary data.

#### Output

| order_id | order_date | status    | dbt_audit_in_a | dbt_audit_in_b | dbt_audit_row_status | dbt_audit_num_rows_in_status | dbt_audit_sample_number |
|----------|------------|-----------|----------------|----------------|----------------------|------------------------------|-------------------------|
| 1        | 2024-01-01 | completed | True           | True           | identical            | 1                            | 1                       |
| 2        | 2024-01-02 | completed | True           | False          | modified             | 2                            | 1                       |
| 2        | 2024-01-02 | returned  | False          | True           | modified             | 2                            | 1                       |
| 3        | 2024-01-03 | completed | True           | False          | modified             | 2                            | 2                       |
| 3        | 2024-01-03 | completed | False          | True           | modified             | 2                            | 2                       |
| 4        | 2024-01-04 | completed | False          | True           | added                | 1                            | 1                       |

Note that there are 4 rows with the `modified` status, but `dbt_audit_num_rows_in_status` says 2. This is because it is counting each primary key only once.

#### Arguments

- `a_query` and `b_query`: The queries you want to compare.
- `primary_key_columns` (required): A list of primary key column(s) used to join the queries together for comparison.
- `columns` (required): The columns present in the two queries you want to compare.
- `sample_limit`: Number of sample records to return per status. Defaults to 20.

#### Usage

```sql

{% set old_query %}
  select
    id as order_id,
    amount,
    customer_id
  from old_database.old_schema.fct_orders
{% endset %}

{% set new_query %}
  select
    order_id,
    amount,
    customer_id
  from {{ ref('fct_orders') }}
{% endset %}

{{ 
  audit_helper.compare_and_classify_query_results(
    old_query, 
    new_query, 
    primary_key_columns=['order_id'], 
    columns=['order_id', 'amount', 'customer_id']
  )
}}

```

### compare_and_classify_relation_rows ([source](macros/compare_and_classify_relation_rows.sql))

A wrapper to `compare_which_query_columns_differ`, except it takes two [Relations](https://docs.getdbt.com/reference/dbt-classes#relation) (instead of two queries).

Each relation must have the same columns with the same names, but they do not have to be in the same order.

#### Arguments

- `a_relation` and `b_relation`: The [relations](https://docs.getdbt.com/reference/dbt-classes#relation) you want to compare.
- `primary_key_columns` (required): A list of primary key column(s) used to join the queries together for comparison.
- `columns` (optional): The columns present in the two queries you want to compare. Build long lists with a few exclusions with `dbt_utils.get_filtered_columns_in_relation`, or pass `None` and the macro will find all intersecting columns automatically.
- `sample_limit`: Number of sample records to return per status. Defaults to 20.

#### Usage

```sql

{% set old_relation = adapter.get_relation(
      database = "old_database",
      schema = "old_schema",
      identifier = "fct_orders"
) -%}

{% set dbt_relation = ref('fct_orders') %}

{{ audit_helper.compare_and_classify_relation_rows(
    a_relation = old_relation,
    b_relation = dbt_relation,
    primary_key_columns = ["order_id"],
    columns = None
) }}

```

### quick_are_queries_identical ([source](macros/quick_are_queries_identical.sql))

On supported adapters (currently Snowflake and BigQuery), take a hash of all rows in two queries and compare them.

This can be calculated relatively quickly compared to other macros in this package and can efficiently provide reassurance that a refactor introduced no changes.

#### Output

| are_tables_identical |
|----------------------|
| true                 |

#### Arguments

- `a_query` and `b_query`: The queries you want to compare.
- `columns` (required): The columns present in the two queries you want to compare.

#### Usage

```sql

{% set old_query %}
    select * from old_database.old_schema.dim_product
{% endset %}

{% set new_query %}
    select * from {{ ref('dim_product') }}
{% endset %}

{{ audit_helper.compare_column_values(
    a_query = old_query,
    b_query = new_query,
    columns=['order_id', 'amount', 'customer_id']
  ) 
}}

```

### quick_are_relations_identical ([source](macros/quick_are_relations_identical.sql))

A wrapper to `quick_are_queries_identical`, except it takes two [Relations](https://docs.getdbt.com/reference/dbt-classes#relation) (instead of two queries).

Each relation must have the same columns with the same names, but they do not have to be in the same order. Build long lists with a few exclusions with `dbt_utils.get_filtered_columns_in_relation`, or pass `None` and the macro will find all intersecting columns automatically.

#### Usage

```sql

{% set old_relation = adapter.get_relation(
      database = "old_database",
      schema = "old_schema",
      identifier = "fct_orders"
) -%}

{% set dbt_relation = ref('fct_orders') %}

{{ audit_helper.quick_are_relations_identical(
    a_relation = old_relation,
    b_relation = dbt_relation,
    columns = None
) }}

```

### compare_row_counts ([source](macros/compare_row_counts.sql))

This macro does a simple comparison of the row counts in two relations.

#### Output

Calling this macro on two different relations will return a very simple table comparing the row counts in each relation.

| relation_name                                | total_records  |
|----------------------------------------------|---------------:|
| target_database.target_schema.my_a_relation  |     34,231     |
| target_database.target_schema.my_b_relation  |     24,789     |

#### Arguments

- `a_relation` and `b_relation`: The [Relations](https://docs.getdbt.com/reference/dbt-classes#relation) you want to compare.

#### Usage

```sql

{% set old_relation = adapter.get_relation(
      database = "old_database",
      schema = "old_schema",
      identifier = "fct_orders"
) -%}

{% set dbt_relation = ref('fct_orders') %}

{{ audit_helper.compare_row_counts(
    a_relation = old_relation,
    b_relation = dbt_relation
) }}

```

## Compare Columns

### compare_which_query_columns_differ ([source](macros/compare_which_query_columns_differ.sql))

This macro generates SQL that can be used to detect which columns returned by two queries contain _any_ value level changes.

It does not return the magnitude of the change, only whether or not a difference has occurred. Only records that exist in both queries (as determined by the primary key) are considered.

#### Output

The generated query returns whether or not each column has any differences:

| column_name | has_difference |
|-------------|----------------|
| order_id    | False          |
| customer_id | False          |
| order_date  | True           |
| status      | False          |
| amount      | True           |

#### Arguments

- `a_query` and `b_query`: The queries to compare
- `primary_key_columns` (required): A list of primary key column(s) used to join the queries together for comparison.
- `columns` (required): The columns present in the two queries you want to compare.

### compare_which_relation_columns_differ ([source](macros/compare_which_relation_columns_differ.sql))

A wrapper to `compare_which_query_columns_differ`, except it takes two [Relations](https://docs.getdbt.com/reference/dbt-classes#relation) (instead of two queries).

Each relation must have the same columns with the same names, but they do not have to be in the same order. Build long lists with a few exclusions with `dbt_utils.get_filtered_columns_in_relation`, or pass `None` and the macro will find all intersecting columns automatically.

#### Usage

```sql

{% set old_relation = adapter.get_relation(
      database = "old_database",
      schema = "old_schema",
      identifier = "fct_orders"
) -%}

{% set dbt_relation = ref('fct_orders') %}

{{ audit_helper.compare_which_columns_differ(
    a_relation = old_relation,
    b_relation = dbt_relation,
    primary_key_columns = ["order_id"],
    columns = None
) }}

```

```sql

{% set old_relation = adapter.get_relation(
      database = "old_database",
      schema = "old_schema",
      identifier = "fct_orders"
) -%}

{% set dbt_relation = ref('fct_orders') %}

{% set columns = dbt_utils.get_filtered_columns_in_relation(old_relation, exclude=["loaded_at"]) %}

{{ audit_helper.compare_which_columns_differ(
    a_relation = old_relation,
    b_relation = dbt_relation,
    primary_key_columns = ["order_id"],
    columns = columns
) }}

```

### compare_column_values ([source](macros/compare_column_values.sql))

This macro generates SQL that can be used to compare a column's values across two queries. This macro is useful when you've used the `compare_which_query_columns_differ` macro to identify a column with differing values and want to understand how many discrepancies are caused by that column.

#### Output

The generated query returns a summary of the count of rows where the column's values:

- match perfectly
- differ
- are null in `a` or `b` or both
- are missing from `a` or `b`

| match_status                | count  | percent_of_total |
|-----------------------------|-------:|-----------------:|
| ✅: perfect match            | 37,721 | 79.03            |
| ✅: both are null            | 5,789  | 12.13            |
| 🤷: missing from a          | 5     | 0.01             |
| 🤷: missing from b          | 20     | 0.04             |
| 🤷: value is null in a only | 59     | 0.12             |
| 🤷: value is null in b only | 73     | 0.15             |
| ❌: ‍values do not match    | 4,064  | 8.51             |

#### Arguments

- `a_query` and `b_query`: The queries you want to compare.
- `primary_key`: The primary key of the model. Used to sort unmatched results for row-by-row validation. Must be a unique key (unqiue and never `null`) in both tables, otherwise the join won't work as expected.
- `column_to_compare`: The column you want to compare.
- `emojis` (optional): Boolean argument that defaults to `true` and displays ✅, 🤷 and ❌ for easier visual scanning. If you don't want to include emojis in the output, set it to `false`.
- `a_relation_name` and `b_relation_name` (optional): Names of the queries you want displayed in the output. Default is `a` and `b`.

#### Usage

```sql

{% set old_query %}
    select * from old_database.old_schema.dim_product
    where is_latest
{% endset %}

{% set new_query %}
    select * from {{ ref('dim_product') }}
{% endset %}

{{ audit_helper.compare_column_values(
    a_query = old_query,
    b_query = new_query,
    primary_key = "product_id",
    column_to_compare = "status"
) }}

```

### compare_all_columns ([source](macros/compare_all_columns.sql))

Similar to `compare_column_values`, except it can be used to compare _all_ columns' values across two _relations_. This macro is useful when you've used the `compare_queries` macro and found that a significant number of your records don't match and want to understand how many discrepancies are caused by each column.

#### Output

By default, the generated query returns a summary of the count of rows where the each column's values:

- match perfectly
- differ
- are null in `a` or `b` or both
- are missing from `a` or `b`

| column_name  | perfect_match  | null_in_a | null_in_b | missing_from_a | missing_from_b | conflicting_values |
|-------|-------:|------:|-----------------:|------:|------:|------:|
| order_id  | 10 | 0 | 0 | 0 | 0 | 0 |
| order_date  | 2 | 0 | 0 | 0 | 0 | 8 |
| order_status | 6 | 4 | 4 | 0 | 0 | 0 |

Setting the `summarize` argument to `false` lets you check the match status of a specific column value of a specifc row:

| primary_key | column_name | perfect_match  | null_in_a | null_in_b | missing_from_a | missing_from_b | conflicting_values |
|-------|-------|-------:|------:|-----------------:|------:|------:|------:|
| 1 | order_id | true | false | false | false | false | false |
| 1 | order_date | false | false | false | false | false | true |
| 1 | order_status | false | true | true | false | false | false |
| ... | ... | ... | ... | ... | ... | ... | ... |

#### Arguments

- `a_relation` and `b_relation`: The [relations](https://docs.getdbt.com/reference/dbt-classes#relation) you want to compare. Any two relations that have the same columns can be used.
- `primary_key`: The primary key of the model (or concatenated sql to create the primary key). Used to sort unmatched results for row-by-row validation. Must be a unique key (unique and never `null`) in both tables, otherwise the join won't work as expected.
- `exclude_columns` (optional): Any columns you wish to exclude from the validation.
- `summarize` (optional): Allows you to switch between a summary or detailed view of the compared data. Defaults to `true`.

#### Usage

```sql

{% set old_relation = adapter.get_relation(
      database = "old_database",
      schema = "old_schema",
      identifier = "fct_orders"
) -%}

{% set dbt_relation = ref('fct_orders') %}

{{ audit_helper.compare_all_columns(
    a_relation = old_relation,
    b_relation = dbt_relation,
    primary_key = "order_id"
) }}

```

### compare_relation_columns ([source](macros/compare_relation_columns.sql))

This macro generates SQL that can be used to compare the schema (ordinal position and data types of columns) of two relations. This is especially useful when:

- Comparing a new version of a relation with an old one, to make sure that the structure is the same
- Helping figure out why a `union` of two relations won't work (often because the data types are different)

#### Output

| column_name | a_ordinal_position | b_ordinal_position | a_data_type       | b_data_type       | has_ordinal_position_match | has_data_type_match | in_a_only | in_b_only | in_both |
|-------------|--------------------|--------------------|-------------------|-------------------| -------------------------- | ------------------- | --------- | --------- | ------- |
| order_id    | 1                  | 1                  | integer           | integer           |                       True |                True |     False |     False |    True |
| customer_id | 2                  | 2                  | integer           | integer           |                       True |                True |     False |     False |    True |
| order_date  | 3                  | 3                  | timestamp         | date              |                       True |               False |     False |     False |    True |
| status      | 4                  | 5                  | character varying | character varying |                      False |                True |     False |     False |    True |
| amount      | 5                  | 4                  | bigint            | bigint            |                      False |                True |     False |     False |    True |

Note: For adapters other than BigQuery, Postgres, Redshift, and Snowflake, the ordinal position is inferred based on the response from dbt Core's `adapter.get_columns_in_relation()`, as opposed to being loaded from the information schema.

#### Arguments

- `a_relation` and `b_relation`: The [relations](https://docs.getdbt.com/reference/dbt-classes#relation) you want to compare.

#### Usage

```sql

{% set old_relation = adapter.get_relation(
      database = "old_database",
      schema = "old_schema",
      identifier = "fct_orders"
) -%}

{% set dbt_relation = ref('fct_orders') %}

{{ audit_helper.compare_relation_columns(
    a_relation=old_relation,
    b_relation=dbt_relation
) }}

```

## Advanced Usage

### Print Output To Logs

You may want to print the output of the query generated by an audit helper macro to your logc (instead of previewing the results).

To do so, you can alternatively store the results of your query and print it to the logs.

For example, using the `compare_column_values` macro:

```sql
{% set old_query %}
    select * from old_database.old_schema.dim_product
    where is_latest
{% endset %}

{% set new_query %}
    select * from {{ ref('dim_product') }}
{% endset %}

{% set audit_query = audit_helper.compare_column_values(
    a_query = old_query,
    b_query = new_query,
    primary_key = "product_id",
    column_to_compare = "status"
) %}

{% set audit_results = run_query(audit_query) %}

{% if execute %}
{% do audit_results.print_table() %}
{% endif %}
```

The `.print_table()` function is not compatible with dbt Cloud, so an adjustment needs to be made in order to print the results. Add the following code to a new macro file:

```sql
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

To run the macro, execute `dbt run-operation print_audit_output()` in the command bar.

### Use Output For Custom Singular Test

If desired, you can use the audit helper macros to add a dbt test to your project to protect against unwanted changes to your data outputs.

For example, using the `compare_all_columns` macro, you could set up a test that will fail if any column values do not match.

Users can configure what exactly constitutes a value match or failure. If there is a test failure, results can be inspected in the warehouse. The primary key and the column name can be included in the test output that gets written to the warehouse. This enables the user to join test results to relevant tables in your dev or prod schema to investigate the error.

_Note: this test should only be used on (and will only work on) models that have a primary key that is reliably `unique` and `not_null`. [Generic dbt tests](https://docs.getdbt.com/docs/building-a-dbt-project/tests#generic-tests) should be used to ensure the model being tested meets the requirements of `unique` and `not_null`._

To create a test for the `stg_customers` model, create a custom test
in the `tests` subdirectory of your dbt project that looks like this:

```sql
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

The `where not perfect_match` statement is an example of a filter you can apply to define whatconstitutes a test failure. The test will fail if any rows don't meet the requirement of a perfect match. Failures would include:

- If the primary key exists in both relations, but one model has a null value in a column.
- If a primary key is missing from one relation.
- If the primary key exists in both relations, but the value conflicts.

If you'd like the test to only fail when there are conflicting values, you could configure it like this:

```sql
{{ 
  audit_helper.compare_all_columns(
    a_relation=ref('stg_customers'), 
    b_relation=api.Relation.create(database='dbt_db', schema='analytics_prod', identifier='stg_customers'),
    primary_key='id'
  ) 
}}
where conflicting_values
```

If you want to create test results that include columns from the model itself for easier inspection, that can be written into the test:

```sql
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

This structure also allows for the test to group or filter by any attribute in the model or in the macro's output as part of the test, for example:

```sql
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

You can write a `compare_all_columns` test on individual table; and the test will be run as part of a full test suite run - `dbt test --select stg_customers`.

If you want to [store results in the warehouse for further analysis](https://docs.getdbt.com/docs/building-a-dbt-project/tests#storing-test-failures), add the `--store-failures` flag.

## Legacy Macros

### compare_queries ([source](macros/compare_queries.sql))

> [!TIP]
> Consider `compare_and_classify_query_results` instead

This macro generates SQL that can be used to do a row-by-row comparison of two queries. This macro is particularly useful when you want to check that a refactored model (or a model that you are moving over from a legacy system) are identical. `compare_queries` provides flexibility when:

- You need to filter out records from one of the relations.
- You need to rename or recast some columns to get them to match up.
- You only want to compare a small number of columns, so it's easier to write the columns you want to compare, rather than the columns you want to exclude.

#### Output

By default, the generated query returns a summary of the count of rows that are unique to `a`, unique to `b`, and identical:

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

#### Arguments

- `a_query` and `b_query`: The queries you want to compare.
- `primary_key` (optional): The primary key of the model (or concatenated sql to create the primary key). Used to sort unmatched results for row-by-row validation.
- `summarize` (optional): Allows you to switch between a summary or detailed view of the compared data. Accepts `true` or `false` values. Defaults to `true`.
- `limit` (optional): Allows you to limit the number of rows returned when `summarize = False`. Defaults to `None` (no limit).

#### Usage

```sql

{% set old_query %}
  select
    id as order_id,
    amount,
    customer_id
  from old_database.old_schema.fct_orders
{% endset %}

{% set new_query %}
  select
    order_id,
    amount,
    customer_id
  from {{ ref('fct_orders') }}
{% endset %}

{{ audit_helper.compare_queries(
    a_query = old_query,
    b_query = new_query,
    primary_key = "order_id"
) }}

```

### compare_relations ([source](macros/compare_relations.sql))

> [!TIP]
> Consider `compare_and_classify_relation_rows` instead

A wrapper to `compare_queries`, except it takes two [Relations](https://docs.getdbt.com/reference/dbt-classes#relation) (instead of two queries).

Each relation must have the same columns with the same names, but they do not have to be in the same order. Use `exclude_columns` if some columns only exist in one relation.

#### Arguments

- `a_relation` and `b_relation`: The [relations](https://docs.getdbt.com/reference/dbt-classes#relation) you want to compare.
- `primary_key` (optional): The primary key of the model (or concatenated sql to create the primary key). Used to sort unmatched results for row-by-row validation.
- `exclude_columns` (optional): Any columns you wish to exclude from the validation.
- `summarize` (optional): Allows you to switch between a summary or detailed view of the compared data. Accepts `true` or `false` values. Defaults to `true`.
- `limit` (optional): Allows you to limit the number of rows returned when `summarize = False`. Defaults to `None` (no limit).

#### Usage

```sql

{% set old_relation = adapter.get_relation(
      database = "old_database",
      schema = "old_schema",
      identifier = "fct_orders"
) -%}

{% set dbt_relation = ref('fct_orders') %}

{{ audit_helper.compare_relations(
    a_relation = old_relation,
    b_relation = dbt_relation,
    exclude_columns = ["loaded_at"],
    primary_key = "order_id"
) }}

```

## Internal Macros

Macros prefixed with an `_` (such as those in the `utils/` subdirectory) are for private use. They are not documented or contracted and can change without notice.
