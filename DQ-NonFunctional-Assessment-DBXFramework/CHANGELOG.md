# Changelog

All notable changes to `databricks-dq-framework` are documented here.

---

## [1.0.37] — 2026-04-09

### Changed
- **`field_rule_summary()` — removed `Rank` column**
  The `Rank` column (always `1`) added no analytical value and cluttered Excel pivots.
  Removed from `ConfigManager.field_rule_summary()` return schema and empty-schema fallback.

---

## [1.0.36] — 2026-04-07

### Added
- **`field_rule_summary()` — flat config audit DataFrame**
  Returns one row per active rule for a field (or all fields), covering all four rule types
  in a single unpivoted DataFrame — suitable for `.display()`, Excel export, pivot, and filter.

  Columns: `FullFieldName`, `DataCategory`, `TargetSchemaName`, `TargetTableName`,
  `TargetFieldName`, `RuleType`, `PatternCategory`, `PatternPriority`, `PatternName`,
  `PatternDescription`, `PatternValue`, `Status`

  Rule types as rows:
  - `"Data Length"` — L01 min/max character length from `configFieldValues`
  - `"Value Range"` — L04 min/max data value from `configFieldValues` (only when configured)
  - `"Pattern Rule"` — L03 patterns fully resolved via `resolve_patterns()` at individual
    pattern-name level; sorted by `PatternPriority`; `Status` = `"Allowed"` / `"Not Allowed"`
  - `"Custom Rule"` — L02 custom queries from `configCustomQuery`; `Status` = `"Must Match"`
    / `"Must NOT Match"`; `PatternCategory` = `"CustomQuery (SQL|REGEX|PYTHON)"`

  Input: logical `FullFieldName`, physical `Schema.Table.Column`, or omit for all fields.
  Multi-field reuse visible: one logical rule → multiple physical `TargetFieldName` rows.

  - `ConfigManager.field_rule_summary(field_name=None)` added to `config.py`
  - `dq.field_rule_summary(field_name=None)` passthrough added to `DQFramework`
  - Listed in `guide()` step 5
  - New cell added to `03-run.py`

---

## [1.0.35] — 2026-04-07

### Added
- **`summary_by_violation_type()` — new reporting helper**
  Groups `v_auditDQChecks` by `FullFieldName`, `GeneratedOn`, `ExecutionID`,
  `TargetSchemaName`, `TargetTableName`, `TargetFieldName`, `ViolationType`
  and returns a `RecordCount` per group.  Useful for understanding which violation
  categories are most prevalent across fields and tables.

  - `query_summary_by_violation_type()` added to `reporting/views.py`
  - `dq.summary_by_violation_type(exec_id)` method added to `DQFramework`
  - Listed in `guide()` step 5 alongside the other result helpers
  - New cell added to `03-run.py` in the View Results section

---

## [1.0.34] — 2026-04-07

### Fixed
- **`prepare_curated_table()` / `_execute_field_assessment()` — `DELTA_REPLACE_WHERE_MISMATCH` (root cause)**
  `replaceWhere` enforces that every row written must satisfy the predicate.
  After assigning a UUID, `DQRowID IS NOT NULL` — so written rows always violate
  `DQRowID IS NULL`, causing `DELTA_REPLACE_WHERE_MISMATCH` regardless of the source filter.

  Fixed by disabling `spark.databricks.delta.replaceWhere.constraintCheck.enabled`
  for the duration of the write (restored in a `finally` block).  This is the documented
  Databricks escape hatch for intentional data transformations during `replaceWhere`.
  Delta still physically replaces only the `DQRowID IS NULL` rows; all other rows remain
  untouched at the storage level.

---

## [1.0.33] — 2026-04-07

### Fixed
- **`prepare_curated_table()` / `_execute_field_assessment()` — `DELTA_REPLACE_WHERE_MISMATCH` (initial attempt)**
  The `replaceWhere` source included non-null `DQRowID` rows via `.otherwise(col("DQRowID"))`,
  which violated the `DQRowID IS NULL` predicate check.
  Fixed by filtering the source to only `DQRowID IS NULL` rows before assigning UUIDs —
  ensuring no pre-existing non-null rows were included in the write.

---

## [1.0.32] — 2026-04-07

### Fixed
- **`prepare_curated_table()` / `_execute_field_assessment()` — `INVALID_NON_DETERMINISTIC_EXPRESSIONS` when populating `DQRowID`**
  Delta's `UPDATE` statement rejects `uuid()` entirely — the engine raises
  `INVALID_NON_DETERMINISTIC_EXPRESSIONS` before execution regardless of the `WHERE`
  clause.  Both `prepare_curated_table()` and the NULL-guard in `_execute_field_assessment()`
  used `UPDATE {table} SET DQRowID = uuid() WHERE DQRowID IS NULL`.

  Fixed by replacing the `UPDATE` with a DataFrame `replaceWhere` partial overwrite:
  - Read the table as a DataFrame.
  - Assign `uuid()` via `withColumn` only where `DQRowID IS NULL`.
  - Write back with `.option("replaceWhere", "DQRowID IS NULL")` — Delta atomically
    replaces only the matching rows; all other rows (including existing DQ results)
    are untouched.
  - `F` (`pyspark.sql.functions`) import added to `framework.py`.

## [1.0.31] — 2026-04-07

### Fixed
- **`prepare_curated_table()` — idempotent NULL `DQRowID` population**
  The early-return path (all 4 DQ columns already present) previously skipped the
  UUID population step.  Rows inserted after the initial `prepare_curated_tables()` call
  would have `DQRowID = NULL`, causing them to silently skip the write-back MERGE
  (`NULL = NULL` is never true in SQL).
  Fixed: NULL `DQRowID` population now always runs regardless of whether columns were
  newly added.

- **`_execute_field_assessment()` — NULL `DQRowID` auto-populate guard**
  Rows with `DQRowID = NULL` at assessment time are now detected and filled before
  `checked_df` is computed, with a table re-read to capture the assigned IDs.

- **`_execute_field_assessment()` — docstring updated**
  Removed stale references to `DeltaTable.update()`; now describes the two-pass MERGE
  pattern keyed on `t.DQRowID = s.DQRowID`.

---

## [1.0.30] — 2026-04-07

### Fixed
- **`run_assessment` — `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` when curated table has duplicate rows**
  The DQ write-back MERGE was keyed on all non-DQ columns (`target.col <=> source.col`).
  When a curated table has fully-duplicate rows (same values across every natural column),
  Delta sees multiple source rows targeting the same destination row and aborts.

  Root cause: the SQL Server proc used `UPDATE A … CROSS APPLY fn(field) B` which
  locates rows by physical RID, not column equality — duplicate rows are updated
  independently.  Delta MERGE has no physical row identity; it matches only on the
  ON condition, so column-equality joins break for duplicate rows.

  Fixed by adding `DQRowID STRING` to each curated table via `prepare_curated_tables()`.
  `DQRowID` is populated with `uuid()` — globally unique, race-condition safe, and
  works correctly under parallel assessments.  The write-back MERGE now joins on
  `t.DQRowID = s.DQRowID`, guaranteeing 1:1 row matching.  `checked_df` (already
  computed for audit collection) carries `DQRowID` from the same table scan.

### Changed
- **`prepare_curated_tables()` / `prepare_curated_table()`** — now adds four DQ columns
  (`DQRowID`, `DQEligible`, `DQViolations`, `DQFields`) instead of three.
  `DQRowID` is immediately populated with `uuid()` for all existing rows.
  Idempotent — re-run after inserting new rows to fill any NULL `DQRowID` values.

---

## [1.0.29] — 2026-04-07

### Added
- **`verify_config()` — duplicate `_ID` and duplicate logical rule checks**
  Scans all user-managed config tables for duplicate `_ID` values (root cause of
  `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` in config MERGEs) and
  checks `configFieldAllowedPattern` for duplicate
  `(FullFieldName, PatternCategory, PatternSubCategory, PatternName)` combinations
  that could produce contradictory `IsPatternAllowed` values.
  Called automatically by `generate_rule_functions()` (raises `RuntimeError`) and
  `show_config_summary()` (prints banner); also exposed as a standalone cell in
  `03-run.py`.

- **`guide()` — updated `verify_config` entry to describe all three check types**

---

## [1.0.28] — 2026-04-07

### Fixed
- **`add_pattern_rule()` / `02-config-sql.py` — `COALESCE` replaces `NOT (col <=> val)` in MERGE conditions**
  The `NOT (a <=> b)` null-safe-equals negation syntax caused concern about
  Databricks SQL compatibility.  Replaced with
  `COALESCE(t.col, '') <> COALESCE(s.col, '')` for all three nullable selector
  columns (`PatternCategory`, `PatternSubCategory`, `PatternName`) in both the
  `ConfigManager.add_pattern_rule()` MERGE and the equivalent `configFieldAllowedPattern`
  MERGE block in `02-config-sql.py`.

- **`02-config-sql.py` / `02-config-pyspark.py` — sample data corrections**
  Pattern rule rows 79–89 corrected to `FullFieldName = 'first_name'`
  (previously labelled `'name_general'` in an earlier revision).
  Both the SQL `UNION ALL SELECT` block and the PySpark `add_pattern_rule()` calls
  now reflect the correct field name.

---

## [1.0.27] — 2026-04-07

### Fixed
- **`configFieldAllowedPattern` MERGE `WHEN MATCHED` — silently skipped updates to `FullFieldName`, `PatternCategory`, `PatternSubCategory`, and `PatternName`**
  The original `WHEN MATCHED` condition only compared `IsPatternAllowed` and
  `IsActive`.  Any change to the three-level pattern selector or to `FullFieldName`
  was silently ignored — the row appeared unchanged in the target table while the
  source carried updated values.
  Fixed in both `ConfigManager.add_pattern_rule()` and the `configFieldAllowedPattern`
  MERGE block in `02-config-sql.py`: the condition now detects changes to all six
  mutable columns; `UPDATE SET` applies every column including `LastUpdatedBy` /
  `LastUpdatedOn`.

### Removed
- **`dq-framework -Consume & Test.py`** scratch notebook deleted from `sample_usage/`;
  equivalent coverage is provided by `03-run.py`.

---

## [1.0.26] — 2026-04-07

### Fixed
- **`auditDQChecks` / `statDQChecks` — `_ID` reverted to `GENERATED ALWAYS AS IDENTITY`**
  The pandas-based sequential `_ID` introduced in 1.0.24 added a `MAX(_ID)` query
  per batch and required explicitly writing `_ID` into the insert schema.  On
  Databricks Unity Catalog the `GENERATED ALWAYS AS IDENTITY` column type is
  supported and more reliable.  Reverted DDL to `BIGINT GENERATED ALWAYS AS IDENTITY`
  and removed `_ID` from both the pandas DataFrame schema and the `StructType` —
  Delta now assigns the column automatically on append.

---

## [1.0.25] — 2026-04-07

### Changed
- **`sample_usage()` — bundled file extraction replaces Repos-path resolution**
  The `sample_usage/` folder is now bundled inside the installed package
  (`dq_framework/sample_usage/`).  On first call `sample_usage()` copies all files
  to `/Workspace/Users/{current_user()}/databricks-dq-framework/sample_usage/`
  (private per user; works on serverless and classic compute without a Repos clone).
  Subsequent calls are idempotent — existing files are only overwritten when the
  bundled version is newer.  Raises `RuntimeError` if `current_user()` cannot be
  resolved rather than silently returning a placeholder path.

---

## [1.0.24] — 2026-04-07

### Fixed
- **`auditDQChecks` / `statDQChecks` — `_ID` assignment refactored away from `ROW_NUMBER() OVER (Window)`**
  The Spark `Window` + `withColumn` approach required a shuffle and failed on
  Spark Connect (serverless) with `CANNOT_DETERMINE_TYPE` when any audit column was
  all-`None`.  Replaced with pandas-level sequential assignment (`_ID = max_id + i + 1`,
  where `max_id = COALESCE(MAX(_ID), 0)` on the existing table).  DDL for both tables
  updated from `BIGINT GENERATED ALWAYS AS IDENTITY` to `BIGINT NOT NULL` to reflect
  that the framework now owns `_ID` generation.

### Changed
- **`sample_usage()` — two-candidate path resolution**
  Path now resolved in priority order: (1) sibling `sample_usage/` directory next to
  the installed package (Repos checkout), then (2)
  `/Workspace/Repos/{current_user()}/databricks-dq-framework/sample_usage`.
  Raises `FileNotFoundError` with both paths tried and step-by-step remediation
  instructions if neither exists.

- **Mock CSV data refreshed** — `mock_curated_vendors.csv` and
  `mock_curated_contacts.csv` extended; `mock_curated_locations.csv` updated with
  realistic address data to exercise L01/L03/L04 rules against location fields.

---

## [1.0.23] — 2026-04-07

### Added
- **`01-install.py` — `dq.sample_usage(spark)` and Volume-based CSV loading**
  Install notebook updated to call `dq.sample_usage(spark)` to resolve the sample
  resource path, then copy the three mock CSVs to a Unity Catalog Volume and load
  them as managed Delta tables.  Removes the previous hard-coded
  `/Workspace/Repos/…` path references.

### Fixed
- **`sample_usage()` — `FileNotFoundError` with clear remediation instead of silent empty listing**
  Previously, if the `sample_usage/` folder was not found the method printed an empty
  block with no indication of the problem.  Now raises `FileNotFoundError` with the
  exact paths tried, the resolved username, and Repos setup instructions.

---

## [1.0.22] — 2026-04-06

### Fixed
- **`summary_by_table` — `TypeError: takes 1 positional argument but 2 were given`**
  The method accepted no parameters, so `dq.summary_by_table(exec_id)` raised a `TypeError`.
  Fixed by adding an optional `execution_id` parameter (same pattern as `violations()` and
  `quality_scores()`).  When provided, the result is filtered to that execution only.

- **`auditDQChecks` / `statDQChecks` — `_ID` not unique, not sequential, starts at 0**
  `monotonically_increasing_id()` produces sparse 64-bit integers that are neither
  sequential nor guaranteed unique across partitions — and the first row on partition 0
  gets `_ID = 0`.  Replaced with `ROW_NUMBER() OVER (ORDER BY 1)` seeded from
  `COALESCE(MAX(_ID), 0)` on the existing table, so each append batch gets IDs that
  continue from the last written row (1, 2, 3 … on first run; N+1, N+2 … on subsequent
  runs).

---

## [1.0.21] — 2026-04-06

### Added
- **`dq.sample_usage(spark)`** — self-demo helper
  Resolves the `sample_usage` folder path dynamically via `current_user()` and prints a
  grouped listing of all demo notebooks, sample CSV data files, and the Excel config
  template.  Intended as the first call a new user makes to orient themselves and locate
  the hands-on materials.  Returns the resolved path string for downstream use.

---

## [1.0.20] — 2026-04-06

### Fixed
- **`add_custom_query` — MERGE silently ignored changes to `FullFieldName`, `CustomQueryType`, and `CustomQueryDescription`**
  The `WHEN MATCHED` condition only checked `CustomQuery`, `IsConditionAllowed`, and `IsActive`.
  Changes to `FullFieldName` (e.g. fixing a typo), `CustomQueryType` (e.g. changing from
  `PYTHON` to `REGEX`), or `CustomQueryDescription` were silently ignored — the row was never
  updated, the old values persisted, and no error was raised.
  Fixed by adding all three fields to both the `WHEN MATCHED AND (...)` condition (using
  `IS DISTINCT FROM` for nullable columns) and the `UPDATE SET` clause.

---

## [1.0.19] — 2026-04-06

### Fixed
- **`inspect_checker` — REGEX-type L02 closures showed `type=auto` and expression `?`**
  `getclosurevars(check).nonlocals` only surfaces variables directly referenced in `check`.
  `expression` and `_cqtype` are captured by the nested `_evaluate` closure, not by `check`
  itself — so they were invisible to the top-level introspection call.
  Fixed by retrieving the `_evaluate` function from `check`'s nonlocals and calling
  `getclosurevars(_evaluate).nonlocals` to read the actual expression and type.

### Added
- **`generate_rule_functions()` config pre-flight gate**
  Calls `verify_config()` before loading any config tables.  If the configuration has
  referential integrity issues (unknown FullFieldName, unknown PatternName, or no active
  mappings), a `RuntimeError` is raised immediately with a list of every failing issue and
  a pointer to `dq.config.show_config_summary()`.
  This replaces the previous silent-pass behaviour where bad config was silently ignored
  and produced incomplete or empty checker functions.

- **`show_config_summary()` — louder failure banner**
  When `verify_config()` finds issues, the summary now prints a prominent
  `CONFIG VERIFICATION FAILED` banner (with `!`-border) that explicitly states
  `generate_rule_functions() will raise RuntimeError until resolved`, so the warning
  cannot be missed in notebook output.

---

## [1.0.18] — 2026-04-06

### Fixed
- **`add_custom_query` — Databricks legacy Spark SQL escape mode strips string literals and backslashes**
  Databricks runtime uses legacy Spark SQL string escaping where `\` is the escape character,
  not the ANSI SQL `''` (doubled-quote) standard.
  The previous `expression.replace("'", "''")` produced `''@''` which Spark treated as two
  adjacent empty string literals concatenated — stripping the surrounding quotes and storing
  `@` instead of `'@'`.  Backslash characters in regex patterns (e.g. `\.` for literal dot)
  were also silently dropped because Spark legacy mode interpreted `\.` as an escaped dot.
  Fixed by applying Databricks-compatible escaping in the correct order:
  (1) double all backslashes `\` → `\\`, then (2) escape single quotes `'` → `\'`.
  The same fix is applied to the `description` field in the same method.
  Affects all SQL expressions and regex patterns stored via `add_custom_query`.

---

## [1.0.17] — 2026-04-06

### Fixed
- **`generate_rule_functions` — stale SQL expressions persist after `configCustomQuery` is truncated**
  `register_sql_expressions` was only called when the new SQL expression list was non-empty
  (`if sql_exprs: registry.register_sql_expressions(...)`).
  When all SQL custom queries for a field were removed (e.g. by truncating `configCustomQuery`
  and re-running), the previous entries remained in `_registry._sql_expressions`, causing
  `inspect_checker`, `validate_custom_queries_sql`, and the assessment runner to still see
  the old expressions.
  Fixed by calling `register_sql_expressions` unconditionally — an empty list now correctly
  clears any previously registered SQL expressions for that field.

### Added
- **`dq.validate_custom_queries_sql()`** — pre-flight SQL expression validator
  Validates all registered SQL-type L02 custom queries against Spark SQL before running an
  assessment.  Each expression has `@InputValue` substituted with `CAST(NULL AS STRING)` to
  trigger Spark's parse/analysis phase without scanning any data (the NULL result is intentional
  and irrelevant — only parse success matters).
  Reports pass/fail count per run; for failures prints the checker name, CQ ID, expression
  text, and the Spark parse error with T-SQL → Spark SQL translation hints.
  REGEX/PYTHON-type rules are explicitly noted as not in scope — they compile into closures
  at `generate_rule_functions()` time and any errors surface immediately at that step.
  Returns `True` if all expressions are valid, `False` if any failed.
  Exposed in `dq.guide()` as step 3c in the workflow and in the OTHER METHODS reference list.

---

## [1.0.16] — 2026-04-06

### Fixed
- **DataType checks — NULL input incorrectly fails when `IsPatternAllowed = true`**
  In the SQL proc, every DataType fail branch is gated by `@TargetValue IS NOT NULL`,
  so NULL input always passes through. In Python, `if input_value else ''` converted
  `None` to empty string `''`, which is `not None`, so the `is_allowed=True` fail
  path fired for NULL values.
  Fixed in all six affected DataType checks (`Is Fully Numeric`, `Is Fully Decimal`,
  `Is Boolean`, `Is Time`, `Is Date`, `Is Timestamp`) by switching to
  `if input_value is None: return _pass(...)` before the string normalisation step.

- **`resolve_pattern_rules.py` — `p_from_cat` always evaluated to truthy**
  Line compared `mp.get("PatternCategory") == mp.get("PatternCategory")` (self-comparison,
  always `True`). Fixed to compare against the config row's `cat` variable.

---

## [1.0.15] — 2026-04-06

### Fixed
- **`_apply_sql_l02_checks` — invalid SQL expressions crash assessment instead of being skipped**
  In Databricks Spark Connect, `F.expr().schema` is deferred — plan analysis
  (including `PARSE_SYNTAX_ERROR`) is not triggered until the first action, which
  fires outside the `try/except` block and crashes the whole assessment run.
  Fixed by validating each expression via `spark.sql(...)` with `@InputValue`
  substituted by `CAST(NULL AS STRING)` before applying it to the DataFrame.
  Invalid expressions (e.g. T-SQL `LOCATE(@, col)` / unquoted `LIKE %x%`) are
  now caught and skipped with a clear warning that names the common T-SQL→Spark
  SQL differences, instead of crashing the assessment.

---

## [1.0.14] — 2026-04-06

### Fixed
- **`check_is_date` — dateutil too permissive, falsely identifies address strings as dates**
  `dateutil.parser.parse` accepted strings like `"3982 2nd St"` as valid dates
  (interpreting `3982` as a year, `2nd` as the 2nd day), triggering a false DQ
  violation for address fields configured with `Is Date: NOT Allowed`.
  SQL Server's `ISDATE()` correctly rejects such strings.
  Fixed by replacing the dateutil call with strict structural regex patterns that
  mirror the formats SQL Server's `ISDATE()` recognises:
  ISO (`2024-01-15`), US/UK slash (`01/15/2024`), dash (`15-01-2024`),
  named-month (`15 Jan 2024`, `Jan 15, 2024`), compact (`20240115`),
  EU dot-separated (`15.01.2024`).

---

## [1.0.12] — 2026-04-06

### Fixed
- **`DELTA_FAILED_TO_MERGE_FIELDS` on `statDQChecks.LoggedOn`** — DDL defines
  `LoggedOn` as `TIMESTAMP` but the writer used `DateType()` schema and
  `pd.Timestamp(date.today())`. Fixed to `TimestampType()` and
  `pd.Timestamp(datetime.utcnow())` to match the table schema exactly.

---

## [1.0.8] — 2026-04-02

### Fixed
- **`generate_rule_functions` — fields with only L01/L02/L04 rules now get checkers**
  Previously, only fields present in `configFieldAllowedPattern` (L03) were picked up.
  Fields with custom queries or length/value range rules but no pattern rules were silently
  skipped. The field source list now unions all three active config tables
  (`configFieldAllowedPattern` + `configFieldValues` + `configCustomQuery`).

- **L01/L02/L04 rules silently dropped for generic FullFieldNames (no dots)**
  The lookup key was built as `f"{schema}.{table}.{field}"` which produced
  `"email_address.."` (two trailing dots) for single-part names. Config table
  rows keyed as `"email_address"` were never found, so L01/L02/L04 rules were
  silently ignored. Fixed to `".".join(p for p in [schema, table, field] if p)`
  which correctly reconstructs both `"email_address"` and `"Schema.Table.Field"`.

- **`_make_fn_name` — trailing underscores on generic field names**
  `'email_address'` (no dots) produced `fn_DQ_email_address__` because empty
  `schema` and `table` segments were still joined. Fixed by filtering empty parts:
  `"fn_DQ_" + "_".join(p for p in [schema, table, field] if p)`.

- **`generate_rule_functions` warning — wrong column queried from `mapDQChecks`**
  The post-generation warning block was selecting `FullFieldName` from `mapDQChecks`,
  which does not have that column. Fixed to use `DQFunctionName`.

- **`run_assessment` — silent "no mappings" message**
  The "No active mappings found" path previously only logged to the Python logger
  (invisible in Databricks notebooks). It now prints a visible message including the
  active scope filter and the total row count of `mapDQChecks`, so users can distinguish
  between an empty table and a filter mismatch.

- **`_parse_ffn` helper — not importable from `framework.py`**
  `_parse_ffn` was a nested function inside `generate_rule_functions()` and could not
  be imported by `framework.py`. Moved to module level in `generate_rule_functions.py`.

### Added
- **`dq.prepare_curated_tables(schema_name=None)`** — reads all distinct
  `(TargetCatalogName, TargetSchemaName, TargetTableName)` from active `mapDQChecks`
  rows and adds `DQEligible`, `DQViolations`, `DQFields` columns to each.
  Idempotent. Requires ALTER privilege on each target table.

- **`dq.prepare_curated_table(schema_name, table_name, catalog_name=None)`** —
  same as above but for a single table.

- **`dq.test_checker(fn_name, *values)`** — executes a registered field checker
  against one or more test values and prints the pass/fail outcome, violation
  type, and the full diagnostic log message produced by the closure. The log
  messages (embedded in every L01/L02/L03/L04 check) state exactly which rule
  ran and why it passed or failed — this is the primary debugging tool for
  unexpected assessment outcomes.

- **`dq.inspect_checker(fn_name, show_all_patterns=False)`** — introspects the
  in-memory closure for a registered field checker and prints every compiled rule:
  L01 length bounds, L02 custom queries (Python + SQL) with expressions and
  allow/block flags, L03 pattern counts grouped by category (or listed individually
  with `show_all_patterns=True`), and L04 value range bounds.

- **Pre-flight column check in `run_assessment`** — raises a clear `RuntimeError`
  naming the missing DQ columns and pointing to `prepare_curated_table()` rather
  than crashing deep inside Spark query planning with an unresolved column error.

- **`dq.guide()`** — prints full workflow, FullFieldName patterns, all `ConfigManager`
  methods, and validation levels in one readable block for quick reference.

- **`✓ Assessment complete — ExecutionID: …`** print at the end of every
  `run_assessment()` call (both the "no mappings" early-exit path and the normal path).

- **`generate_rule_functions()` pretty print** — lists every registered checker as
  `• fn_DQ_<name>  [N SQL expr]` after generation completes.

---

## [1.0.7] and earlier

Initial open-source releases. See git log for details.
