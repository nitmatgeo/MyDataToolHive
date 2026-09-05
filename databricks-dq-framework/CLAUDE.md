# DQ Framework — Project Context

## What this folder is

A **Databricks-native Data Quality Assessment Framework** — a self-creating Python package
that runs configurable field-level DQ checks against Unity Catalog Delta tables.

Ported from a SQL Server / stored-procedure DQ framework.  Deployed as a Python wheel
(`databricks_dq_framework`) and consumed in Databricks notebooks.

Current version: see `pyproject.toml`.

---

## Files in this folder

| File / Path | Purpose |
|-------------|---------|
| `dq_framework/framework.py` | `DQFramework` class — main entry point |
| `dq_framework/ddl_framework_tables.py` | `DDL_STATEMENTS` dict + `TABLE_ORDER` — all table DDL |
| `dq_framework/seed_master_data.py` | Seeds 27 categories + 118 patterns via INSERT-ONLY MERGE |
| `dq_framework/config.py` | `ConfigManager` — Python API for user-managed config tables |
| `dq_framework/engine/generate_rule_functions.py` | Compiles one checker closure per field |
| `dq_framework/engine/resolve_pattern_rules.py` | L0A CTE equivalent — pattern precedence resolution |
| `dq_framework/engine/data_assessment_rules.py` | `DQRunner` — executes checks, writes audit + stats |
| `dq_framework/reporting/views.py` | Reporting views + query helper functions |
| `dq_framework/sample_usage/` | Sample notebooks (01-install, 02-config-sql, 03-run) + Excel template |

---

## Architecture

```
DQFramework(spark, catalog, schema="dq")
    └── setup()                        → schema + tables + views + seed (idempotent)
    └── dq.config (ConfigManager)
    │       └── register_field()
    │       └── set_field_values()
    │       └── block_category() / allow_pattern() / block_pattern() / add_pattern_rule()
    │       └── add_custom_query() / add_custom_query_regex() / add_custom_query_sql()
    │       └── add_mapping()
    │       └── verify_config()        → pre-flight: dup _ID, dup rules, FK integrity
    │       └── show_config_summary()  → row counts + config health banner
    │       └── field_rule_summary()   → flat DataFrame of all active rules (Excel-ready)
    └── generate_rule_functions()      → compiles Python checker closures per field
    │                                    calls verify_config() — raises RuntimeError on failure
    └── validate_custom_queries_sql()  → pre-flight: dry-run all SQL-type custom queries
    └── prepare_curated_tables()       → adds DQRowID / DQEligible / DQViolations / DQFields
    └── run_assessment(schema=...)     → executes checks, MERGE write-back on DQRowID
    └── violations(exec_id)            → DataFrame of all violations
    └── quality_scores(exec_id)        → DataFrame of pass/fail % per field
    └── summary_by_violation_type()    → RecordCount grouped by ViolationType + field
    └── summary_by_table(exec_id)      → TableQualityPct per curated table
    └── fields_below_threshold(pct)    → fields below quality threshold
    └── field_rule_summary(field=None) → flat config audit DataFrame (all rule types)
    └── inspect_checker(fn_name)       → prints compiled rule breakdown for a field
    └── add_invalid_keyword()          → custom keyword to masterPattern (_ID >= 1000)
    └── guide()                        → prints step-by-step usage guide
    └── sample_usage(spark)            → extracts bundled sample notebooks to Workspace
```

---

## Naming conventions — MUST follow exactly

### Schema
- **Default:** `schema="dq"` in constructor.
- Deployed project uses `databricks_hackathon` schema (override at init time).
- **Never mix** with ETL framework's `etl` schema.

### Tables — PascalCase, no prefix

```
masterDataCategory        [FRAMEWORK-MANAGED — 27 data type classifications]
masterPattern             [FRAMEWORK-MANAGED — 118 built-in patterns; custom _ID >= 1000]
masterField               [USER-MANAGED — logical source fields registered for assessment]
configFieldValues         [USER-MANAGED — length (L01) and value range (L04) boundaries]
configFieldAllowedPattern [USER-MANAGED — pattern allow/block rules (L03)]
configCustomQuery         [USER-MANAGED — custom SQL/regex/Python validators (L02)]
mapDQChecks               [USER-MANAGED — maps logical fields to physical curated columns]
auditDQChecks             [RESULTS — row-level violations (Result=FALSE only)]
statDQChecks              [RESULTS — aggregated pass/fail stats per execution]
```

### Columns — PascalCase
```
_ID, FullFieldName, DataCategoryTypeID, FieldID
PatternName, PatternCategory, PatternSubCategory, PatternPriority, PatternValue, PatternDescription
MinDataLength, MaxDataLength, MinDataValue, MaxDataValue
TargetCatalogName, TargetSchemaName, TargetTableName, TargetFieldName
DQFunctionSchemaName, DQFunctionName
CustomQuery, CustomQueryType, IsConditionAllowed, CustomQueryDescription
IsPatternAllowed, IsActive
ExecutionID, MappingID, InputValue, Result, ViolationType, LogMessage
RowsQualified, RowsDisqualified
GeneratedOn, LoggedOn
CreatedBy, CreatedOn, LastUpdatedBy, LastUpdatedOn
```

### DQ columns added to curated tables (by `prepare_curated_tables()`)
```
DQRowID      STRING   — UUID per row; stable MERGE join key
DQEligible   BOOLEAN  — True=all checks passed, False=any failed, NULL=not assessed
DQViolations STRING   — "[field: ViolationType], ..." accumulated across all fields
DQFields     STRING   — "[field1], [field2]" all assessed fields on this row
```

**DQEligible is STICKY** — once False, stays False regardless of later field results.
**DQViolations and DQFields are APPEND-ONLY** — each field assessment appends to existing.

### Primary keys
- User/framework-managed tables: `_ID INT NOT NULL` (user-assigned)
- Results tables: `_ID BIGINT GENERATED ALWAYS AS IDENTITY`

### masterPattern ID ranges
- Framework reserves `_ID 1–999` (118 built-in patterns)
- Custom/project patterns: `_ID >= 1000`

### FQN — always backtick-quoted
```python
def _fqn(self, table_name: str) -> str:
    if self.catalog:
        return f"`{self.catalog}`.`{self.dq_schema}`.`{table_name}`"
    return f"`{self.dq_schema}`.`{table_name}`"
```

---

## Check types (L-codes)

| Code | Table | What it checks |
|------|-------|---------------|
| L01 | `configFieldValues` | Data length — `MinDataLength` to `MaxDataLength` chars |
| L02 | `configCustomQuery` | Custom SQL expression, regex, or Python validator |
| L03 | `configFieldAllowedPattern` | Pattern allow/block rules from `masterPattern` |
| L04 | `configFieldValues` | Value range — `MinDataValue` to `MaxDataValue` |

---

## CustomQueryType values

| Value | Behaviour |
|-------|-----------|
| `SQL` | Spark SQL expression; `@InputValue` replaced with field value |
| `REGEX` | Python `re.search()`; `@InputValue` replaced; case-insensitive |
| `PYTHON` | Name of a registered validator function (via `register_validator()`) |
| `NULL` | Auto-detect — tries SQL first, then REGEX |

`IsConditionAllowed = True` → value **must match** to pass.
`IsConditionAllowed = False` → value **must NOT match** to pass.

---

## Pattern Precedence Resolution (L03) — Critical

Mirrors the **L0A CTE RANK() window function** from the original SQL Server proc.
Implemented in `dq_framework/engine/resolve_pattern_rules.py`.

### Three-level hierarchy — most specific wins:
```
PatternName (specificity=3) > PatternSubCategory (specificity=2) > PatternCategory (specificity=1)
```

### How a config row expands:
One `configFieldAllowedPattern` row can target:
- **Category-level**: PatternCategory set, SubCat=NULL, Name=NULL → expands to ALL patterns in that category
- **SubCategory-level**: SubCat set, Name=NULL → expands to all patterns in that subcategory
- **Pattern-level**: PatternName set → targets exactly one pattern

### Deduplication (Rank=1 rule):
Partition by `(FullFieldName, PatternValue, PatternName)`.
Sort by: **specificity DESC**, then **IsPatternAllowed DESC** (Allowed=1 > NotAllowed=0).

**"More specific overrides broader; within same specificity, Allowed overrides Not Allowed."**

### Example:
```
Config A: Category=SpecialCharacter, IsPatternAllowed=0  → blocks ALL ~40 special chars
Config B: PatternName="Has At Sign",  IsPatternAllowed=1  → allows @ specifically

For "@": Config B wins (specificity=3 > 1) → Allowed
For "!": Only Config A applies → Not Allowed
```

### PatternPriority — evaluation ORDER, not precedence:
Controls display/evaluation sequence (lower = earlier). Does NOT affect which rule wins.
Typical values: Data=1, DataType1=2-3, SpecialCharacter=25-30, InvalidData=40-42, UnicodeCharacter=43.

---

## DQRowID — UUID stable row identifier

**Why it exists:** Delta MERGE requires a unique join key. Joining on all content columns
fails for fully-duplicate rows (`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`).
Mirrors SQL Server's physical row identity (RID/slot pointer) behaviour.

**Population:** `prepare_curated_tables()` adds the column and fills it. Re-running is safe
— only NULL rows are touched (idempotent). Assessment engine also auto-fills NULLs as a guard.

**Delta UPDATE cannot use `uuid()` — raises `INVALID_NON_DETERMINISTIC_EXPRESSIONS`.**
Fixed using `replaceWhere` partial overwrite with constraint check temporarily disabled:
```python
spark.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", "false")
try:
    (spark.table(fqn)
     .filter(F.col("DQRowID").isNull())
     .withColumn("DQRowID", F.expr("uuid()"))
     .write.format("delta")
     .option("replaceWhere", "DQRowID IS NULL")
     .mode("overwrite")
     .saveAsTable(fqn))
finally:
    spark.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", "true")
```
`replaceWhere` physically replaces only NULL-DQRowID rows; all others are untouched.
The constraint check is disabled because after UUID assignment `DQRowID IS NOT NULL` by design
(written rows no longer satisfy the predicate — this is intentional, not a bug).

---

## Assessment write-back mechanics

Two-pass pattern (mirrors SQL Server proc's AuditQueryText + QueryText):

**Pass 1 (audit):** Read curated table → apply checker UDF → collect Result=False rows
→ INSERT into `auditDQChecks` + `statDQChecks`.

**Pass 2 (write-back):** MERGE on `t.DQRowID = s.DQRowID` → update DQEligible / DQViolations / DQFields.
`checked_df` (same scan as Pass 1) carries DQRowID — no extra table read needed.

DQEligible merge logic — sticky False:
```python
F.when(F.col("t.DQEligible") == False, F.lit(False)).otherwise(F.col("s._dq_check.result"))
```

---

## verify_config() — three check types

Called automatically by `generate_rule_functions()` (raises RuntimeError on failure) and
`show_config_summary()` (prints warning). Also callable standalone.

1. **Duplicate `_ID`** in masterField, configFieldValues, configFieldAllowedPattern,
   configCustomQuery, mapDQChecks — causes MERGE errors.
2. **Duplicate logical rules** in `configFieldAllowedPattern` — same
   `(FullFieldName, PatternCategory, PatternSubCategory, PatternName)` produces contradictions.
3. **Referential integrity** — FullFieldName in mapDQChecks/configFieldAllowedPattern/
   configCustomQuery must exist in masterField; PatternName must exist in masterPattern.

Returns: `{"ok": bool, "issues": list[str]}`

---

## field_rule_summary() — flat config audit DataFrame

Returns one row per active rule across all rule types. Suitable for `.display()`, Excel export, pivot.

Columns: `FullFieldName`, `DataCategory`, `TargetSchemaName`, `TargetTableName`, `TargetFieldName`,
`RuleType`, `PatternCategory`, `PatternPriority`, `PatternName`, `PatternDescription`, `PatternValue`, `Status`

| RuleType | Source | Status values |
|----------|--------|---------------|
| `"Data Length"` | configFieldValues (L01) | `"Enforced"` |
| `"Value Range"` | configFieldValues (L04) | `"Enforced"` (only when min/max configured) |
| `"Pattern Rule"` | configFieldAllowedPattern + masterPattern (L03) | `"Allowed"` / `"Not Allowed"` |
| `"Custom Rule"` | configCustomQuery (L02) | `"Must Match"` / `"Must NOT Match"` |

L03 rows are fully resolved via `resolve_patterns()` — post-precedence, one row per winning pattern.
Input: logical `FullFieldName`, physical `Schema.Table.Column`, or omit for all fields.

---

## Key rules — always apply

1. **Databricks SQL syntax only** — not T-SQL (`LEN` → `LENGTH`, `CHARINDEX` → `INSTR`, etc.).
2. **PascalCase** for all table and column names — no snake_case.
3. **INSERT-ONLY MERGE** for all config writes — `WHEN NOT MATCHED THEN INSERT`.
4. **COALESCE in MERGE ON** for nullable string keys — not `IS DISTINCT FROM` or `NOT (col <=> val)`.
5. **Idempotency everywhere** — `CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`.
6. **masterPattern custom rows: `_ID >= 1000`**.
7. **`FullFieldName` = `Schema.Table.Column`** — three parts, dot-separated.
8. **`prepare_curated_tables()` before `run_assessment()`** — never ALTER DQ columns manually.
9. **DQRowID must never be NULL at assessment time** — engine auto-fills but always run prepare first.
10. **Delta UPDATE cannot use `uuid()`** — use the `replaceWhere` + `constraintCheck.enabled=false` pattern.
11. **`verify_config()` before `run_assessment()`** — called implicitly by `generate_rule_functions()`.
12. **Excel formula defaults**: null-equivalents (`GETDATE()`, `current_user()`, `NULL`, blank) → Python `None`. Row 6 as default row reference.

---

## Complete assessment flow (correct order)

```python
# 1. Setup (idempotent)
dq.setup()

# 2. Verify config
dq.config.show_config_summary()
result = dq.config.verify_config()   # standalone check

# 3. Generate rule functions (also runs verify_config internally)
dq.generate_rule_functions()

# 3b. Optional — validate SQL custom queries before running
ok = dq.validate_custom_queries_sql()

# 4. Prepare curated tables (adds 4 DQ columns — idempotent)
dq.prepare_curated_tables()

# 5. Run
exec_id = dq.run_assessment(schema_name="<curated_schema>")

# 6. Results
dq.violations(exec_id).display()
dq.quality_scores(exec_id).display()
dq.summary_by_violation_type(exec_id).display()
dq.summary_by_table(exec_id).display()
dq.fields_below_threshold(threshold=80).display()

# Config audit
dq.field_rule_summary("my_field").display()
dq.field_rule_summary().display()
```

---

## Package build and publish

```bash
python -m build
build_and_publish.bat
```

Installed on clusters via:
```python
%pip install databricks_dq_framework
```
