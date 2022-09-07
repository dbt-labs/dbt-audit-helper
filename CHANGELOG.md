# audit_helper 0.6.0
🚨 This version requires dbt Core 1.2 or above, and is ready for dbt utils 1.0.

Changed:
* add column_name to output of compare_column_values by @leoebfolsom in https://github.com/dbt-labs/dbt-audit-helper/pull/47
* Easier switching between summary and details by @christineberger in https://github.com/dbt-labs/dbt-audit-helper/pull/52
* Removes references to dbt_utils for cross-db macros

New features:
* dbt Cloud instructions for compare_column_values by @SamHarting in https://github.com/dbt-labs/dbt-audit-helper/pull/45
* Compare all columns macro by @leoebfolsom in https://github.com/dbt-labs/dbt-audit-helper/pull/50


# audit_helper 0.5.0
This version brings full compatibility with dbt-core 1.0. It requires any version (minor and patch) of v1, which means far less need for compatibility releases in the future.

# audit_helper 0.4.1
🚨 This is a compatibility release in preparation for `dbt-core` v1.0.0 (🎉). Projects using this version with dbt-core v1.0.x can expect to see a deprecation warning. This will be resolved in the next minor release.