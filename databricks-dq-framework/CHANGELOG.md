# Changelog

All notable changes to `databricks-dq-framework` are documented here.

---

## [1.1.1] — current

- Stability and documentation fixes
- Corrected `register_field` argument order: `full_field_name` (string) first, followed by `data_category_type_id` and `field_id`

## [1.0.0] — first stable release

- Metadata-driven, field-level data quality assessment on Databricks Unity Catalog Delta tables
- Fluent configuration API: `register_field` → `set_field_values` → `block_category` → `allow_pattern` → `add_mapping` — all chainable
- `FullFieldName` format: `Schema.Table.Column` as primary key throughout
- Validator types: min/max length, regex pattern, allowed/blocked character categories, custom SQL, custom Python
- `generate_rule_functions()` compiles configured rules; `run_assessment()` executes and returns an execution ID
- Violations logged with full row-level audit — data is never transformed or fixed
- INSERT-ONLY MERGE for all config writes — idempotent
- PyPI trusted publishing (OIDC) via GitHub Actions

## [0.x] — initial pre-release iterations

Early development and internal testing releases.
