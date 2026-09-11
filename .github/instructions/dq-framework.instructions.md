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
    │       └── block_pattern / add_pattern_rule
    │       └── add_custom_query / add_custom_query_regex / add_custom_query_sql / add_mapping
    │       └── verify_config()        → dup _ID + dup logical rules + FK integrity
    │       └── show_config_summary()  → row counts + health banner
    │       └── field_rule_summary()   → flat Excel-ready config audit DataFrame
    └── generate_rule_functions()      → compile checker closures; calls verify_config()
    └── validate_custom_queries_sql()  → dry-run SQL custom queries before assessment
    └── prepare_curated_tables()       → add DQRowID / DQEligible / DQViolations / DQFields
    └── run_assessment(schema=...)     → run all checks, MERGE write-back on DQRowID
    └── violations / quality_scores / summary_by_violation_type / summary_by_table
    └── fields_below_threshold / field_rule_summary
    └── inspect_checker(fn_name, show_all_patterns=False)
    │                                  → print compiled rule breakdown for a field
    └── test_checker(fn_name, *values) → run checker against test values + print pass/fail + log message
    └── register_validator(name, fn)   → register a Python callable for L02 PYTHON custom queries
    └── add_invalid_keyword(keyword, pattern_id, priority, description)
    │                                  → add project keyword to masterPattern (_ID >= 1000)
    └── add_custom_pattern(pattern_id, pattern_category, pattern_name, ...)
    │                                  → add any custom validation pattern to masterPattern
    └── guide()                        → prints step-by-step usage guide to stdout
    └── sample_usage(spark)            → extracts bundled sample notebooks to Workspace

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

## FullFieldName — two valid patterns

**Pattern A — Reusable (recommended for generic checks):**
`FullFieldName = "email_address"` — a short logical name. Define rules once; map to
as many physical columns and tables as needed via `mapDQChecks`. Use when the same
validation logic applies across multiple tables.

**Pattern B — Column-specific:**
`FullFieldName = "Schema.Table.Column"` — exactly three dot-separated parts. Rules
are tied to one exact column. Use when a column has unique constraints not shared elsewhere.

Both patterns can coexist. Mix freely. `field_rule_summary("email_address")` accepts either form.

---

## Register a field and configure DQ rules

```python
dq.config \
    .register_field(<_id>, "email_address",      # Pattern A — reusable
                    data_category_type_id=<category_id>) \
    # OR: .register_field(<_id>, "Schema.Table.Column", ...)   # Pattern B — column-specific

    .set_field_values(<_id>, "email_address",
                      min_data_length=<n>, max_data_length=<n>,
                      min_data_value=None, max_data_value=None) \
    .block_category(<_id>, "email_address", "<PatternCategory>") \
    .allow_pattern(<_id>, "email_address", "<PatternName>") \
    .add_mapping(<_id>, "email_address",
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

Convenience wrappers (preferred):
```python
dq.config.add_custom_query_regex(<_id>, "Schema.Table.Column",
                                 r"^[A-Z]{2}[0-9]{6}$", must_match=True,
                                 description="2 uppercase + 6 digits")
dq.config.add_custom_query_sql(<_id>, "Schema.Table.Column",
                               "@InputValue IN ('ACTIVE', 'INACTIVE')",
                               is_condition_allowed=True,
                               description="Must be valid status")
```

---

## Pattern precedence

```
PatternName (specificity=3) > PatternSubCategory (specificity=2) > PatternCategory (specificity=1)
Within same specificity: Allowed (1) overrides Not Allowed (0)
```

**PatternPriority** controls evaluation ORDER (lower = earlier) — it does NOT affect which rule wins when two rules conflict. Typical values: Data=1, DataType1=2-3, SpecialCharacter=25-30, InvalidData=40-42.

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

## Debugging a checker

```python
# See what rules are compiled into a field checker
dq.inspect_checker("fn_DQ_email_address")
dq.inspect_checker("fn_DQ_email_address", show_all_patterns=True)  # list every L03 pattern

# Run test values through the checker and see exact pass/fail + log message
dq.test_checker("fn_DQ_email_address",
                "user@example.com",  # expected PASS
                "not-an-email",      # expected FAIL
                "",                  # empty — expected FAIL
                None)                # NULL — expected PASS (not applicable)
```

## run_assessment — full signature

```python
exec_id = dq.run_assessment(
    schema_name="Curated",     # None = all schemas
    table_name=None,           # None = all tables in schema
    field_name=None,           # None = all fields in table
    reset_eligible_flag=False, # True = clear prior DQ flags and exit (no assessment)
    enable_output=True,        # False = suppress violation/score display
    execution_id=None,         # supply a fixed UUID for grouped tracking
)
```

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
