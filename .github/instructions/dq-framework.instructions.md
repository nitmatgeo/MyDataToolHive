---
applyTo: "databricks-dq-framework/**"
---

# Databricks DQ Assessment Framework — Copilot Instructions

You are working on the **Databricks DQ Assessment Framework** in `databricks-dq-framework/`.

Start by reading `databricks-dq-framework/CLAUDE.md` — it contains the full naming conventions, table structure, DQRowID mechanics, pattern precedence rules, and all key decisions.

---

## Role

This framework assesses data quality on Unity Catalog Delta tables by running configurable field-level checks (length, range, pattern, custom SQL/regex/Python). It does **not** transform or fix data — it only assesses and records violations.

---

## Architecture

```
DQFramework(spark, catalog="main", schema="dq")
    └── setup()                        → schema + 9 tables + 2 views + seed (idempotent)
    └── dq.config (ConfigManager)
    │       └── register_field / set_field_values / block_category / allow_pattern
    │       └── add_custom_query / add_mapping
    │       └── verify_config()        → dup _ID + dup logical rules + FK integrity
    │       └── show_config_summary()  → row counts + health banner
    │       └── field_rule_summary()   → flat Excel-ready config audit DataFrame
    └── generate_rule_functions()      → compile checker closures; calls verify_config()
    └── validate_custom_queries_sql()  → dry-run SQL custom queries before assessment
    └── prepare_curated_tables()       → add DQRowID / DQEligible / DQViolations / DQFields
    └── run_assessment(schema=...)     → run all checks, MERGE write-back on DQRowID
    └── violations / quality_scores / summary_by_violation_type / summary_by_table
    └── fields_below_threshold / field_rule_summary
    └── inspect_checker(fn_name)       → print compiled rule breakdown for a field

Tables (in <catalog>.<schema>):
  masterDataCategory        [FRAMEWORK-MANAGED — 27 type classifications]
  masterPattern             [FRAMEWORK-MANAGED — 118 built-in patterns; custom >= 1000]
  masterField               [USER-MANAGED — logical field definitions]
  configFieldValues         [USER-MANAGED — L01 length + L04 value range]
  configFieldAllowedPattern [USER-MANAGED — L03 pattern allow/block rules]
  configCustomQuery         [USER-MANAGED — L02 SQL/regex/Python validators]
  mapDQChecks               [USER-MANAGED — logical field → physical curated column]
  auditDQChecks             [RESULTS — row-level violations (failures only)]
  statDQChecks              [RESULTS — aggregated pass/fail stats]
```

---

## Complete assessment flow

```python
# 1. Setup
dq.setup()

# 2. Pre-flight
dq.config.show_config_summary()
dq.config.verify_config()              # checks dup _IDs, dup rules, FK integrity
dq.generate_rule_functions()           # also calls verify_config() internally
ok = dq.validate_custom_queries_sql()  # optional — validates SQL custom queries

# 3. Prepare curated tables (idempotent — adds DQRowID + 3 DQ columns)
dq.prepare_curated_tables()

# 4. Run
exec_id = dq.run_assessment(schema_name="<curated_schema>")

# 5. Results
dq.violations(exec_id).display()
dq.quality_scores(exec_id).display()
dq.summary_by_violation_type(exec_id).display()
dq.summary_by_table(exec_id).display()
dq.fields_below_threshold(threshold=80).display()

# Config audit (anytime — no assessment needed)
dq.field_rule_summary("my_field").display()
dq.field_rule_summary().display()
```

---

## Register a field and configure DQ rules

```python
dq.config \
    .register_field(<_id>, "Schema.Table.Column",
                    data_category_type_id=<category_id>) \
    .set_field_values(<_id>, "Schema.Table.Column",
                      min_data_length=<n>, max_data_length=<n>,
                      min_data_value=None, max_data_value=None) \
    .block_category(<_id>, "Schema.Table.Column", "<PatternCategory>") \
    .allow_pattern(<_id>, "Schema.Table.Column", "<PatternName>") \
    .add_mapping(<_id>, "Schema.Table.Column",
                 target_schema_name="<schema>",
                 target_table_name="<table>",
                 target_field_name="<column>",
                 target_catalog_name="<catalog>")
```

`FullFieldName` is always `Schema.Table.Column` — three parts, dot-separated.

---

## Add a custom SQL / regex / Python validator (L02)

```python
# SQL — Spark SQL expression (@InputValue placeholder)
dq.config.add_custom_query(
    _id=<n>, full_field_name="Schema.Table.Column",
    custom_query="@InputValue IN ('ACTIVE', 'INACTIVE', 'PENDING')",
    custom_query_type="SQL",
    description="Must be a valid status code",
    is_condition_allowed=True
)

# REGEX
dq.config.add_custom_query(
    _id=<n>, full_field_name="Schema.Table.Column",
    custom_query="^[A-Z]{2}[0-9]{6}$",
    custom_query_type="REGEX",
    description="Must match 2 uppercase letters + 6 digits",
    is_condition_allowed=True
)

# PYTHON — registered validator function name
dq.config.add_custom_query(
    _id=<n>, full_field_name="Schema.Table.Column",
    custom_query="validate_email",
    custom_query_type="PYTHON",
    description="Must be a valid email address",
    is_condition_allowed=True
)
```

Always specify `custom_query_type` explicitly — do not rely on auto-detect.

---

## Pattern precedence

```
PatternName (most specific) > PatternSubCategory > PatternCategory (broadest)
Within same specificity: Allowed overrides Not Allowed
```

Classic pattern:
```python
# Block all special characters, then punch exceptions
dq.config.block_category(1, "Source.Table.Field", "SpecialCharacter")
dq.config.allow_pattern(2, "Source.Table.Field", "Has At Sign")
dq.config.allow_pattern(3, "Source.Table.Field", "Has Hyphen")
```

---

## DQRowID — important constraints

- Added by `prepare_curated_tables()` — never add manually.
- **Delta `UPDATE` cannot use `uuid()`** — use `replaceWhere` + `constraintCheck.enabled=false` pattern.
- Assessment engine MERGE joins on `t.DQRowID = s.DQRowID`.
- NULL DQRowID rows silently skip the MERGE — always run `prepare_curated_tables()` first.

---

## Extend the schema

- Add DDL to `DDL_STATEMENTS` in `dq_framework/ddl_framework_tables.py`
- Add table to `TABLE_ORDER` in the correct position
- For already-deployed tables: generate `ALTER TABLE ... ADD COLUMNS (...)`
- Update `create_reporting_views()` in `reporting/views.py` if views need the new column

---

## Rules — always apply

- **PascalCase** for all table and column names — no snake_case.
- **`FullFieldName` = `Schema.Table.Column`** — always three dot-separated parts.
- **`_ID` user-assigned** for config tables; `BIGINT GENERATED ALWAYS AS IDENTITY` for results.
- **INSERT-ONLY MERGE** for all config writes — `WHEN NOT MATCHED THEN INSERT`.
- **COALESCE in MERGE ON** for nullable string keys.
- **masterPattern custom rows: `_ID >= 1000`** — framework reserves 1–999.
- **`prepare_curated_tables()` before `run_assessment()`** — never ALTER DQ columns manually.
- **`CustomQueryType`** must be `SQL`, `REGEX`, `PYTHON`, or `NULL` explicitly.
- **Databricks SQL syntax only** — not T-SQL.
- **`verify_config()` before `run_assessment()`** — implicit via `generate_rule_functions()`.
- **Use `<catalog>` placeholder** in SQL examples — never hardcode a real catalog name.

---

## Response format

- Lead with the code block — explanation after.
- For field registration, always show the full chain: `register_field()` → `set_field_values()` → `block_category()` / `allow_pattern()` → `add_mapping()`.
- When adding a custom query, always specify `CustomQueryType` explicitly.
- For schema changes, show both the `DDL_STATEMENTS` edit and the `ALTER TABLE` statement.
- When referencing DQRowID population, always mention the `replaceWhere` + `constraintCheck` pattern.
