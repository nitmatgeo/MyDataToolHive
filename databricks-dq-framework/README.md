# Databricks DQ Assessment Framework

A Python library for running **automated, configurable data quality checks** on Databricks Delta tables. Point it at your curated data, configure rules once, and it will check every row for empty fields, invalid formats, junk placeholder values, wrong data types, and custom business rules — then write the results directly back to the table and into a full audit log.

---

> [!NOTE]
> **Get started in 2 minutes**
>
> ```bash
> pip install databricks_dq_framework
> ```
>
> In a Databricks notebook:
>
> ```python
> %pip install databricks_dq_framework
> dbutils.library.restartPython()
> ```
>
> Then extract the sample notebooks, CSV data, and Excel config template directly into your workspace:
>
> ```python
> from dq_framework import DQFramework
> dq = DQFramework(spark, catalog="<your_catalog>", schema="dq")
> dq.sample_usage(spark)   # extracts 00-infrastructure through 03-run, sample CSVs, and Excel config template into your Workspace
> dq.guide()               # prints step-by-step usage guide to stdout
> ```
>
> Run `00-infrastructure.py` first to create the catalog and DQ schema, then `01-install.py` to install the framework on the cluster, upload sample data to a Volume, load it as Delta tables, and call `setup()` — which creates all 9 Delta tables, 2 reporting views, and seeds 27 data categories and 118 built-in validation patterns. Both are idempotent and safe to re-run.

---

## What it does

For each field you register, the framework applies up to four levels of checks across every row of a curated Delta table. It adds four columns to each row: a stable row identifier (`DQRowID`), a pass/fail flag (`DQEligible`), the list of violations (`DQViolations`), and the list of assessed fields (`DQFields`). Every violation is also logged to a dedicated audit table — so you always know which rows failed, which field, and exactly why.

**It does not modify your data.** It only reads, assesses, and records.

---

## Why you need it

Curated data has quality problems that are invisible until they break something downstream: email addresses without an `@`, phone numbers containing letters, dates stored as text, `"null"` or `"N/A"` treated as real values, fields that should never be empty left blank. Without a systematic check, these issues reach BI dashboards, ML models, and downstream consumers undetected.

This framework gives you:

- **Visibility** — know exactly which rows fail, which field, and why — not just "there are bad records"
- **Traceability** — every violation logged with field name, violation type, execution ID, and timestamp
- **Repeatability** — run the same ruleset against every load; quality scores tracked over time
- **Reusability** — configure email rules once, apply automatically to every table that has an email column
- **No code deploys for rule changes** — all rules live in Delta config tables; update and re-run

---

## When to use it

After data lands in your curated / silver Delta tables, before it reaches BI reports, ML features, or downstream consumers:

- **After each ETL load** — schedule alongside your pipeline or trigger it on completion
- **When onboarding a new data source** — configure rules once; every future load is automatically assessed
- **As a promotion gate** — only rows with `DQEligible = True` move to the gold / consumption layer
- **For data governance** — the audit log is a durable, queryable record of data quality over time

---

## Key Features

| Feature | Detail |
|---|---|
| **118 built-in patterns** | 10 categories: special characters, empty/null values, invalid keywords, data types, casing, unicode, duplicated characters |
| **4 check levels per field** | Length range → custom expressions → pattern rules → value range; early exit on first failure |
| **Custom expressions** | Plain regex, Spark SQL with `@InputValue`, or named Python validators |
| **Excel config workbook** | Fill in the bundled spreadsheet → copy formula-generated Python or SQL → run; no manual escaping needed |
| **Chainable Python API** | `ConfigManager` — register, configure, and map fields in a few lines; no raw SQL required |
| **Metadata-driven rules** | All rules live in Delta tables — change config and re-run, no code deploy needed |
| **Idempotent everywhere** | `setup()`, `prepare_curated_tables()`, all config writes — all safe to re-run at any time |
| **Full audit log** | `auditDQChecks` + `statDQChecks` Delta tables, plus two reporting views |
| **Quality scores** | Pass/fail % per field, per table, per execution — with threshold alerting |
| **AI-assisted config** | Auto-suggest rules from a table schema; system prompt for ChatGPT / Copilot / Azure OpenAI |

---

## How it works

```
Config tables (Delta) ──► resolve pattern precedence ──► generate Python checkers
                                                                    │
                                                         run checks row by row (UDFs)
                                                                    │
                                                    ┌───────────────┴───────────────┐
                                               DQ columns                      audit tables
                                        (DQRowID, DQEligible,           (auditDQChecks,
                                       DQViolations, DQFields)           statDQChecks)
                                                                                    │
                                                                         Reporting views +
                                                                         Python helpers
```

---

## Quick Start

```python
from dq_framework import DQFramework

# 1. Initialise
dq = DQFramework(spark, catalog="main", schema="dq")
dq.setup()

# 2. Configure a field (ConfigManager API — no SQL required)
cfg = dq.config
cfg.register_field(1, "Source.SRC_ContactPoint.EMAIL_ADDRESS", data_category_type_id=4)
cfg.set_field_values(1, "Source.SRC_ContactPoint.EMAIL_ADDRESS", min_data_length=6, max_data_length=255)
cfg.block_category(1, "Source.SRC_ContactPoint.EMAIL_ADDRESS", "DataEmptiness")
cfg.allow_pattern(2, "Source.SRC_ContactPoint.EMAIL_ADDRESS", "Has At Sign")
cfg.add_custom_query_regex(1, "Source.SRC_ContactPoint.EMAIL_ADDRESS",
                           r"^[^@\s]+@[^@\s]+\.[^@\s]+$", must_match=True)
cfg.add_mapping(1, "Source.SRC_ContactPoint.EMAIL_ADDRESS",
                target_schema_name="Curated", target_table_name="Entity_Email_Denorm",
                target_field_name="EMAIL_ADDRESS", target_catalog_name="main")

# 3. Verify config before generating
cfg.verify_config()

# 4. Generate field-level checker functions from config
dq.generate_rule_functions()

# 4b. Optional: validate SQL-type custom queries before assessment
dq.validate_custom_queries_sql()

# 5. Add DQ columns to curated tables (idempotent — run once before first assessment)
dq.prepare_curated_tables()

# 6. Run the DQ assessment
exec_id = dq.run_assessment(schema_name="Curated")

# 7. Query results
dq.violations(exec_id).display()
dq.quality_scores(exec_id).display()
dq.summary_by_violation_type(exec_id).display()
dq.summary_by_table(exec_id).display()
dq.fields_below_threshold(threshold=80).display()

# Config audit — see all rules configured for a field (no assessment needed)
dq.field_rule_summary("Source.SRC_ContactPoint.EMAIL_ADDRESS").display()
```

See [sample_usage/](sample_usage/) for hands-on demo notebooks, sample CSV data, and the Excel config template.

---

## Built-in Usage Guide

```python
dq.guide()          # Full workflow, FullFieldName patterns, all ConfigManager methods
dq.sample_usage(spark)  # Print path to demo notebooks, sample data, and Excel template
```

---

## Prerequisite: DQ Columns on Curated Tables

Each curated Delta table being assessed must have four extra columns added before assessment.
Use the built-in helper — it reads `mapDQChecks` and adds them to every mapped table at once:

```python
dq.prepare_curated_tables()                  # all tables referenced in mapDQChecks
dq.prepare_curated_table("Schema", "Table")  # or a single table
```

Both methods are **idempotent** (safe to re-run) and require **ALTER privilege** on each target table.
The assessment runner will raise a clear error if columns are missing, pointing to these methods.

The four columns added are:

```sql
DQRowID      STRING   -- UUID per row — internal join key used by the assessment engine (do not modify)
DQEligible   BOOLEAN  -- true=all checks passed, false=at least one failed, NULL=not yet assessed
DQViolations STRING   -- "[field: ViolationType], ..." — accumulated violations across all assessed fields
DQFields     STRING   -- "[field1], [field2]" — all fields assessed on this row
```

| Column | Value | Meaning |
|---|---|---|
| `DQRowID` | UUID string | Stable row identifier — added once, never changes |
| `DQEligible` | `NULL` | Row not yet assessed |
| `DQEligible` | `True` | All configured checks passed |
| `DQEligible` | `False` | At least one check failed (sticky — stays False once set) |
| `DQViolations` | e.g. `[email_address: Data Type]` | Violated rules (accumulates across fields) |
| `DQFields` | e.g. `[email_address], [phone]` | All fields assessed on this row |

---

## Package Structure

```
dq_framework/
├── __init__.py                        # Exports: DQFramework, ConfigManager
├── framework.py                       # DQFramework facade — top-level orchestration
├── ddl_framework_tables.py            # ≡ Script_00_DDL_Framework_Tables — Delta table DDL (9 tables)
├── seed_master_data.py                # ≡ Script_01_Master_Reference_Data — 27 categories + 118 patterns
├── config.py                          # ConfigManager — Python API for all 5 config tables
├── engine/
│   ├── pattern_checks.py              # All 118 pattern check implementations
│   ├── resolve_pattern_rules.py       # Pattern precedence (L0A CTE equivalent)
│   ├── generate_rule_functions.py     # ≡ p_DQ_GenerateRuleFunctions — FunctionRegistry + closures
│   └── data_assessment_rules.py       # ≡ p_DQ_DataAssessmentRules — DQRunner
├── reporting/
│   └── views.py                       # v_auditDQChecks + v_statDQChecks equivalents
└── agents/
    ├── __init__.py                    # Exports SYSTEM_PROMPT, DQ_TOOLS, suggest_config
    ├── context.py                     # LLM system prompt + short context
    ├── tools.py                       # OpenAI-compatible tool definitions + execute_tool_call()
    └── suggest.py                     # Schema-based config auto-suggester (no LLM required)

dq_framework/sample_usage/             # Bundled inside the PyPI package — extracted to /Workspace/Users/{you}/...
├── 00-infrastructure.py               # One-time: create catalog/schema/volume
├── 01-install.py                      # Per-session: %pip install + dq.setup() + copy CSVs to volume + load as Delta
├── 02-config-pyspark.py               # Seed all config tables via Python ConfigManager methods
├── 02-config-sql.py                   # Same config, seeded via SQL MERGE statements
├── 03-run.py                          # generate_rule_functions → run_assessment → reporting
├── mock_curated_contacts.csv          # Sample data — contacts
├── mock_curated_locations.csv         # Sample data — locations
├── mock_curated_vendors.csv           # Sample data — vendors
└── DBX DQ Framework -Data Model.xlsx # Excel config authoring template (see Excel Template section)
                                       # Run dq.sample_usage(spark) to extract all files to your user workspace
```

> **Getting sample files:** Run `dq.sample_usage(spark)` — it extracts the bundled sample notebooks,
> CSV data, and Excel template to `/Workspace/Users/{you}/databricks-dq-framework/sample_usage/`
> automatically. No Repos clone needed. Works on serverless and all compute types.

---

## 9 Framework Tables

All tables live in the `dq` schema (configurable). Three categories:

| Category | Tables |
|---|---|
| **Framework-Seeded** (auto-populated by `setup()`) | `masterDataCategory`, `masterPattern` |
| **User-Managed** (populated via `ConfigManager` or SQL) | `masterField`, `configFieldValues`, `configFieldAllowedPattern`, `configCustomQuery`, `mapDQChecks` |
| **Results** (auto-populated by `run_assessment()`) | `auditDQChecks`, `statDQChecks` |

| Table | Purpose |
|---|---|
| `masterDataCategory` | 27 field type classifications |
| `masterPattern` | 118 built-in validation patterns. Custom patterns use `_ID >= 1000` (IDs 1–999 are reserved for built-ins) |
| `masterField` | Source fields registered for assessment |
| `configFieldValues` | Data length (L01) and value range (L04) boundaries |
| `configFieldAllowedPattern` | Pattern allow/block rules per field |
| `configCustomQuery` | Custom validation expressions — plain regex, Spark SQL, or named validators (L02) |
| `mapDQChecks` | Maps source field → curated table column |
| `auditDQChecks` | Row-level violation audit log (failures only, append-only) |
| `statDQChecks` | Aggregated pass/fail statistics per execution (append-only) |

> **`generate_rule_functions()` output**: Prints a `•` line for each registered checker with its SQL expression count. Any DQ function referenced in `mapDQChecks` that has no rules configured (in `configFieldValues`, `configFieldAllowedPattern`, or `configCustomQuery`) is flagged with ⚠ and skipped by the assessment runner — avoiding misleading 100% quality scores for unchecked fields.

---

## Validation Levels (in order, early-exit on first failure)

Each generated field checker implements a 5-level hierarchy:

| Level | Check Type | Priority |
|---|---|---|
| **L01** | Data Length range (min/max character count) | Checked first |
| **L02** | Custom expression rules (regex, Spark SQL, or named validator) | Checked second |
| **L03** | 118-pattern checks (in PatternPriority order) | Checked third |
| **L04** | Data Value range (min/max value comparison) | Checked fourth |
| **L99** | Default PASS (if all prior checks pass) | Final |

**Early exit**: The checker returns immediately on first failure.
Re-run after fixing the first violation to surface the next.

---

## 118 Patterns across 10 Categories

| Category | Count | Examples |
|---|---|---|
| `DataType1` | 3 | Is Fully Numeric, Is Fully Decimal, Is Fully Text |
| `DataType2` | 1 | Is AlphaNumeric |
| `DataType3` | 4 | Is Date, Is Time, Is Timestamp, Is Boolean |
| `SpecialCharacter` | 32 | Has Comma, Has At Sign, Has Hyphen, Has Pipe Symbol, … |
| `SpaceFound` | 1 | Has Space |
| `DataEmptiness` | 34 | Is Empty or NULL, Is Virtually Empty with Spaces, … |
| `InvalidKeyword` | 40 | Has Keyword-n/a, Has Keyword-null, Has Keyword-missing, … |
| `FullyDuplicatedCharacter` | 1 | Has Fully Duplicated Character (e.g. "aaaa") |
| `UnicodeCharacters` | 1 | Has Unicode Characters (outside printable ASCII 0x20–0x7E) |
| `CasingCheck` | 2 | Has Lowercase Character, Has Uppercase Character |

**Total: 118 built-in patterns.** Pattern IDs 1–999 are reserved for framework use.
To add custom patterns (e.g. domain-specific invalid keywords), use IDs `>= 1000`.

---

## Configuring Pattern Rules (`configFieldAllowedPattern`)

This table is the heart of the framework — it controls which of the 118 built-in patterns
apply to each field, and whether each pattern is required, blocked, or suppressed.

### The three selector columns

Each row in `configFieldAllowedPattern` points into `masterPattern` via one of three columns.
Populate **one column per row** and leave the other two `NULL`.

| Column | Selects | Patterns activated |
|---|---|---|
| `PatternCategory` | An entire category family | Many — e.g. all 32 `SpecialCharacter` patterns at once |
| `PatternSubCategory` | A named group within a category | Several — e.g. all 8 `Separator` chars: `. , - _ / \ ; :` |
| `PatternName` | One exact pattern by name | Exactly 1 — e.g. only `Has At Sign` |

**Precedence when two rules conflict on the same pattern:** `PatternName` > `PatternSubCategory` > `PatternCategory`.
The most specific rule always wins. Within the same specificity level, `Allowed` wins over `Not Allowed`.

```python
cfg.block_category(1, "Source.T.FIELD", "SpecialCharacter")   # block all 32 special chars
cfg.allow_pattern(2, "Source.T.FIELD", "Has Hyphen")           # except hyphen  (PatternName wins)
cfg.allow_pattern(3, "Source.T.FIELD", "Has Full Stop")        # except .
cfg.allow_pattern(4, "Source.T.FIELD", "Has At Sign")          # except @
```

### When to use each level

**`PatternCategory`** — use when you want a blanket policy for a whole family:
```
SpecialCharacter   Not Allowed  → blocks all 32 special chars in one row
InvalidKeyword     Not Allowed  → blocks all 40 junk keywords (null, n/a, test, missing …)
DataType3          Not Allowed  → blocks values that look like dates, times, or booleans
UnicodeCharacters  Not Allowed  → blocks any character outside printable ASCII (0x20–0x7E)
FullyDuplicatedChar Not Allowed → blocks "aaaaaaa", "--------" etc.
```

**`PatternSubCategory`** — use when you want a subset without naming every pattern:
```
Separator    Allowed      → allows . , - _ / \ ; : all at once (instead of 8 separate rows)
Bracket      Not Allowed  → blocks ( ) [ ] { }
Emptiness    Not Allowed  → blocks NULL, all-spaces, and all 32 "virtually empty with X" patterns
Symbol       Not Allowed  → blocks ! @ # $ % ^ & * ~ + < > ? =
```

Available SubCategories in `masterPattern`:

| Category | SubCategory | Patterns included |
|---|---|---|
| `SpecialCharacter` | `Symbol` | `! @ # $ % ^ & * ~ + < > ? =` (15) |
| `SpecialCharacter` | `Bracket` | `( ) [ ] { }` (6) |
| `SpecialCharacter` | `Separator` | `. , - _ / \ ; : \|` (9) |
| `SpecialCharacter` | `Quote` | `' " \`` (3) |
| `DataEmptiness` | `Null` | `Is Empty or NULL` only |
| `DataEmptiness` | `Spaces` | `Is Virtually Empty with Spaces` only |
| `DataEmptiness` | `Character` | All 32 "Is Virtually Empty with X" patterns |
| `DataEmptiness` | `Emptiness` *(config alias)* | All three Null + Spaces + Character groups |
| `InvalidKeyword` | `Generic` | All 40 junk keywords |

**`PatternName`** — use for precision, one specific pattern:
```
Has At Sign       Allowed      → @ is OK (email local-part)
Has Hyphen        Allowed      → - is OK (compound names, postal codes)
Has Underscore    Allowed      → _ is OK (email, login names)
Has Full Stop     Allowed      → . is OK (email domain separator)
Has Space         Allowed      → space is OK (city, vendor name, address)
Has Space         Not Allowed  → no spaces (email, country code, postal code)
Is Empty or NULL  Allowed      → field is optional — NULL is accepted
Is Empty or NULL  Not Allowed  → field is mandatory — NULL fails
Has Lowercase Character Not Allowed → enforces ALL-CAPS (country code, vendor code)
Is Fully Numeric  Not Allowed  → value must not be a pure number (name fields)
Is Fully Text     Allowed (*)  → value MUST contain no digits (see DataType rules below)
```

### Allowed vs Not Allowed — two different behaviours

The `IsPatternAllowed` flag works differently depending on the pattern category:

#### One-directional patterns (most categories)

These patterns only ask "is the unwanted thing present?".

| `IsPatternAllowed` | Behaviour |
|---|---|
| `0` / Not Allowed | Check fires → **fail** if the pattern is detected in the value |
| `1` / Allowed | Check is **skipped** — the pattern is acknowledged as acceptable, not asserted |

Applies to: `SpecialCharacter`, `SpaceFound`, `CasingCheck`, `DataEmptiness`, `InvalidKeyword`,
`FullyDuplicatedCharacter`, `UnicodeCharacters`.

> `Allowed` here is a **suppressor**, not a validator. It tells the framework
> "this pattern will appear in this field — don't flag it."

#### Bidirectional patterns (`DataType1`, `DataType2`, `DataType3`)

These check type conformance in both directions:

| `IsPatternAllowed` | Behaviour |
|---|---|
| `0` / Not Allowed | **Fail if value IS this type** — e.g. no pure numbers in a name field |
| `1` / Allowed | **Fail if value IS NOT this type** — enforces the type as mandatory |

Examples:

```
Is Fully Numeric  Not Allowed → vendor_name must not be a pure number → flags "99999", "123"
Is Fully Numeric  Allowed     → raw_phone_number must ONLY be digits → flags "+91 9698..." (has +/space)
Is Fully Text     Allowed     → state/city must contain no digits → flags "3H", "1st Avenue"
Is Date           Not Allowed → email_address cannot look like a date → flags "2024-01-15"
Is AlphaNumeric   Not Allowed → state field rejects codes that contain both letters AND digits
```

#### CasingCheck — one-directional only

`Allowed` has no effect on casing checks. Only `Not Allowed` enforces anything.

| Rule | Effect |
|---|---|
| `Has Lowercase Character` Not Allowed | Enforces ALL-CAPS — any `a-z` found = violation |
| `Has Uppercase Character` Not Allowed | Enforces all-lowercase — any `A-Z` found = violation |
| `Has Lowercase Character` Allowed | No effect (check silently suppressed) |

The check uses binary/case-sensitive matching (`COLLATE Latin1_General_BIN` equivalent).
Accented characters (`é`, `ü`, `ñ`) only trigger the Unicode check, not the casing check —
they are outside the `a-z` / `A-Z` ASCII ranges.

### The override rule in practice

The design pattern across every field: **block broadly at category level, then punch
exceptions at pattern level.**

```
email_address — 14 rules:

Category   SpecialCharacter   Not Allowed  → blocks all 32 special chars
Category   DataType3          Not Allowed  → blocks date/time/timestamp/boolean values
Category   InvalidKeyword     Not Allowed  → blocks "null", "n/a", "test" etc.
Category   FullyDupChar       Not Allowed  → blocks "aaaaaaa"
Category   UnicodeCharacters  Not Allowed  → blocks non-ASCII (é, ü, 田中, шрифт)
SubCat     Emptiness          Not Allowed  → blocks NULL, spaces, all virtually-empty patterns
PatternName Has At Sign        Allowed     → overrides SpecialCharacter block → @ is OK  ✓
PatternName Has Hyphen         Allowed     → overrides → - is OK  ✓
PatternName Has Underscore     Allowed     → overrides → _ is OK  ✓
PatternName Has Full Stop      Allowed     → overrides → . is OK  ✓
PatternName Has Space          Not Allowed → spaces are still blocked (no override)  ✓
PatternName Is Empty or NULL   Allowed     → email is optional — NULL accepted  ✓
```

### Typical starting configs by field type

| Field type | Category blocks | Pattern overrides (Allowed) |
|---|---|---|
| Email address | `SpecialCharacter`, `DataType3`, `InvalidKeyword`, `FullyDupChar`, `UnicodeCharacters`, `Emptiness` | `Has At Sign`, `Has Full Stop`, `Has Hyphen`, `Has Underscore` |
| Phone (formatted) | `DataType1`, `DataType2`, `DataType3`, `InvalidKeyword`, `FullyDupChar`, `UnicodeCharacters` | `Has Hyphen`, `Has Plus Sign`, `Has Space`, `Has Open Parenthesis`, `Has Close Parenthesis` |
| Phone (digits-only) | `SpecialCharacter`, `DataType3`, `InvalidKeyword`, `FullyDupChar`, `UnicodeCharacters`, `Emptiness` | `Is Fully Numeric` (Allowed = digits only, mandatory) |
| Postal / ZIP code | `SpecialCharacter`, `DataType3`, `InvalidKeyword`, `FullyDupChar`, `UnicodeCharacters`, `Emptiness` | `Has Hyphen`, `Has Space` |
| Person name | `DataType1`, `DataType2`, `DataType3`, `SpecialCharacter`, `InvalidKeyword`, `FullyDupChar`, `Emptiness` | `Has Hyphen`, `Has Full Stop`, `Has Single Quote`, `Has Space` |
| Vendor / company name | `DataType3`, `SpecialCharacter`, `InvalidKeyword`, `FullyDupChar`, `UnicodeCharacters`, `Emptiness` | `Has Space`, `Has Hyphen`, `Has Full Stop` |
| Country code (2-char) | `DataType1`, `DataType2`, `DataType3`, `SpecialCharacter`, `InvalidKeyword`, `FullyDupChar`, `UnicodeCharacters`, `Emptiness` | `Is Fully Text` (Allowed), add `Has Lowercase Character` Not Allowed for ALL-CAPS |
| Street address | `DataType3`, `InvalidKeyword`, `FullyDupChar`, `UnicodeCharacters`, `Emptiness` | `SpecialChar Separator` (Allowed), `Has Space`, block `Has Exclamation Mark`, `Has Question Mark` |
| Trade / lookup code | `InvalidKeyword`, `FullyDupChar`, `UnicodeCharacters`, `Emptiness`, `100% Numeric` | `SpecialChar-L1`, `SpecialChar-L2`, `Has Semicolon`, `Has Space` |

---

## Custom Validation Rules (L02)

The `configCustomQuery` table stores a `CustomQueryExpression` for each field.
Three options are available.

### Option 1: Plain regex (recommended for non-technical users)

```python
# Must match: value must satisfy the regex to PASS
cfg.add_custom_query_regex(1, "Source.T.EMAIL_ADDRESS",
                           r"^[^@\s]+@[^@\s]+\.[^@\s]+$", must_match=True,
                           description="Basic email format")

# Must NOT match: if the regex matches, the row FAILS
cfg.add_custom_query_regex(2, "Source.T.EMAIL_ADDRESS",
                           r"noemaildress", must_match=False,
                           description="Reject placeholder values")
```

Use `add_custom_query_regex()` to always be explicit about type — it sets `CustomQueryType=REGEX` automatically.

### Option 2: Spark SQL expression

```python
# @InputValue is replaced with the actual column reference at assessment time
cfg.add_custom_query_sql(1, "Source.T.AMOUNT",
                         "CAST(@InputValue AS DOUBLE) > 0",
                         is_condition_allowed=True,
                         description="Amount must be positive")
```

SQL expressions are applied at the DataFrame level via `F.expr()` — not inside a
Python UDF — so they support any Spark SQL built-in function.

### Option 3: Named validator (for complex logic)

Register a Python function once, then reference it by name in any field config:

```python
# Register (once, at cluster startup or in a setup cell)
dq.register_validator("au_mobile", lambda v: bool(__import__('re').match(r'^04[0-9]{8}$', v or '')))

# Reference by name in config
cfg.add_custom_query(_id=3, full_field_name="Source.T.MOBILE_NUMBER",
                     custom_query="au_mobile",
                     is_condition_allowed=True, custom_query_type="PYTHON")
```

Built-in email validators are pre-registered:

| Validator Name | Description |
|---|---|
| `email_basic_format` | Regex: basic `x@y.z` format |
| `email_at_validation` | @ position, count, dot after @ |
| `email_domain_format` | 1–3 dots, no trailing dot, 2+ char labels |
| `email_local_part_dots` | Max 2 dots in local part |
| `email_no_placeholder` | Must not contain 'noemaildress' |
| `email_no_trailing_special` | Must not end with `.`, `-`, or `_` |

---

## Scope Filtering (`1=1` Pattern Preserved)

```python
# All schemas, all tables, all fields
dq.run_assessment()

# Specific schema
dq.run_assessment(schema_name="Curated")

# Specific table
dq.run_assessment(schema_name="Curated", table_name="Individual_Denorm")

# Single field
dq.run_assessment(schema_name="Curated",
                  table_name="Individual_Denorm",
                  field_name="FIRST_NAME")

# Reset previously-failed rows before re-assessment
dq.run_assessment(schema_name="Curated", reset_eligible_flag=True)
```

---

## Reporting

All methods return Spark DataFrames ready for `.display()` or further transformation:

```python
exec_id = dq.run_assessment(schema_name="Curated")

dq.violations(exec_id).display()                   # Row-level violations: field, violation type, value
dq.quality_scores(exec_id).display()               # Pass/fail counts + % per field
dq.summary_by_violation_type(exec_id).display()    # Record count by ViolationType per field — pivot/analysis
dq.summary_by_table(exec_id).display()             # Aggregated quality score per curated table
dq.fields_below_threshold(threshold=80).display()  # Fields with quality < 80%

# Config audit — usable any time, no assessment required
dq.field_rule_summary("Source.Table.FIELD").display()  # All active rules for one field (Excel-ready)
dq.field_rule_summary().display()                      # All rules for all configured fields
```

Two persistent reporting views are also created in the `dq` schema by `setup()`:

| View | Purpose |
|---|---|
| `v_auditDQChecks` | Row-level violations joined with field metadata |
| `v_statDQChecks` | Aggregated pass/fail statistics with `PercentageQualified` |

---

## AI Agent Support

The `dq_framework.agents` module provides three ways to use AI to configure the framework.
**No agent framework (Semantic Kernel, LangChain, AutoGen) is required.**

### Approach A — Auto-suggest from schema (no LLM needed)

```python
from dq_framework.agents import suggest_config

schema = [
    ("EMAIL_ADDRESS", "string"),
    ("FIRST_NAME",    "string"),
    ("POSTAL_CODE",   "string"),
    ("CUSTOMER_ID",   "int"),
]

# Returns ready-to-run ConfigManager code
code = suggest_config(schema, source_schema="Source", source_table="SRC_Customer",
                      curated_schema="Curated", curated_table="Customer_Denorm",
                      catalog="main")
print(code)
```

Or read schema directly from a live Delta table:

```python
schema = spark.table("main.Source.SRC_Customer").dtypes
code = suggest_config(schema, source_schema="Source", source_table="SRC_Customer", ...)
```

### Approach B — Chat assistant with tool calling

All framework operations are exposed as OpenAI-compatible JSON Schema tool definitions.
They work unchanged with OpenAI, Azure OpenAI, Anthropic Claude, Semantic Kernel, AutoGen,
and LangChain.

```python
from dq_framework.agents import SYSTEM_PROMPT, DQ_TOOLS
from dq_framework.agents.tools import execute_tool_call
from openai import AzureOpenAI

client = AzureOpenAI(azure_endpoint="...", api_key="...", api_version="2024-05-01-preview")

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user",   "content": "Configure DQ rules for Source.SRC_Party.EMAIL_ADDRESS"},
    ],
    tools=DQ_TOOLS,
)

for call in response.choices[0].message.tool_calls or []:
    result = execute_tool_call(call.function.name, call.function.arguments, dq, cfg)
    print(f"[Tool] {call.function.name} → {result}")
```

The included `dq_framework/agents/` module has examples and full tool definitions for Azure OpenAI,
Anthropic Claude, Semantic Kernel, LangChain, and AutoGen.
Run `dq.sample_usage(spark)` to get the demo notebooks in your Workspace.

### Approach C — Paste system prompt into ChatGPT / Copilot

```python
from dq_framework.agents import SYSTEM_PROMPT
print(SYSTEM_PROMPT)  # Copy and paste into any chat interface
```

---

## Testing

Unit tests are available in the [source repository (Private)](https://github.com/NitMatGeo/) and run locally with `pytest` — no Databricks cluster, no Spark, no Delta Lake required.

| Test file | What it covers |
|---|---|
| `test_pattern_checks.py` | All major pattern check functions (DataEmptiness, SpecialChar, InvalidKeyword, …) |
| `test_resolve_pattern_rules.py` | Pattern precedence — specific overrides broad, inactive excluded |
| `test_generate_rule_functions.py` | L01/L02/L04 closure building, `FunctionRegistry`, stale function removal |
| `test_seed_master_data.py` | Seed data integrity: 27 categories, 118 patterns, no duplicate IDs |
| `test_suggest.py` | Schema-based config suggester: column classification, code generation |

---

## Platform Requirements

| Requirement | Detail |
|---|---|
| Databricks Runtime | 12.0+ (includes PySpark 3.3+) |
| Delta Lake | 2.0+ |
| Python | 3.9+ |
| Unity Catalog | Recommended (pass `catalog=""` for legacy Hive metastore) |
| python-dateutil | Included in Databricks Runtime; required for full date/time validation |

---

## Adding a New Source Field

1. Register the field: `cfg.register_field(id, "Schema.Table.Column", data_category_type_id=N)`
2. Set length bounds: `cfg.set_field_values(id, ffn, min_data_length=M, max_data_length=N)`
3. Add pattern rules: `cfg.block_category(...)`, `cfg.allow_pattern(...)`, `cfg.block_pattern(...)`
4. Add custom expressions: `cfg.add_custom_query_regex(id, ffn, r"your_regex", must_match=True)`
5. (Optional) Add custom invalid keywords: `dq.add_invalid_keyword("your_keyword", pattern_id=id)`
6. Map to curated column: `cfg.add_mapping(id, ffn, target_schema_name=..., target_table_name=..., ...)`
7. Validate FK integrity: `cfg.verify_config()`
8. Re-run: `dq.generate_rule_functions()`
9. (Optional) Pre-flight SQL validation: `dq.validate_custom_queries_sql()`

---

## Excel Template — Config Authoring Workbook

The workbook (`DBX DQ Framework -Data Model.xlsx`) is bundled inside the package and
extracted to your Workspace when you run `dq.sample_usage(spark)`. It is the recommended
way to author and maintain DQ configurations for any non-trivial field set.

### Why use the template?

Configuring DQ rules manually in Python or SQL for 10+ fields — each with dozens of pattern
allow/block rules, length bounds, and custom expressions — is repetitive, error-prone, and
hard to audit. The Excel template solves this by acting as both a **configuration record**
and a **code generator**:

- One row per rule; columns map directly to framework concepts (field name, pattern category,
  allow/block, expression, description, active flag …)
- Built-in formulas generate ready-to-run Python method calls **and** SQL MERGE rows from the
  same data — no manual escaping or formatting required
- The filled-in workbook becomes a human-readable audit trail of what rules are configured,
  why, and when — something that is hard to get from raw code alone
- Non-developers (data stewards, business analysts) can review, edit, and sign off on rules in
  a familiar spreadsheet before anyone runs a single notebook cell

### Sheets and what to fill in

| Sheet | Maps to table | What to fill in |
|---|---|---|
| `masterField` | `masterField` | Field ID, `FullFieldName` (pattern A or B), `DataCategoryTypeID` |
| `configFieldValues` | `configFieldValues` | Per-field min/max data length (L01) and min/max value range (L04) |
| `configFieldAllowedPattern` | `configFieldAllowedPattern` | Pattern allow/block rules — one row per rule (L03) |
| `configCustomQuery` | `configCustomQuery` | Custom regex, Spark SQL, or named validator expressions (L02) |
| `mapDQChecks` | `mapDQChecks` | Field → curated table column mappings |

### Deciding pattern rules for a field (`configFieldAllowedPattern`)

This is the most complex sheet to fill in and the one where the template adds the most value.
See [Configuring Pattern Rules](#configuring-pattern-rules-configfieldallowedpattern) above for
the full guide — three-column selector rules, Allowed vs Not Allowed behaviour for each
category, CasingCheck specifics, the override mechanism, and ready-made starting configs
by field type.

Quick summary for the Excel sheet:
- One row per rule, one of `PatternCategory` / `PatternSubCategory` / `PatternName` populated per row
- Start with broad category blocks, then add PatternName exceptions (Allowed) to punch holes
- `Allowed` on DataType patterns is a **mandatory type assertion** (value MUST be that type)
- `Allowed` on all other categories is a **suppressor** (skip the check for this pattern)
- A typical email field needs ~10–14 rows; a postal code ~6–8; a name field ~8–12

### Two output columns — M (PySpark) and N (SQL)

Each data sheet has two formula output columns that generate ready-to-paste code:

| Column | Generates | Paste destination |
|---|---|---|
| **M** | `dq.config.<method>(...)` Python call | Directly into a Python notebook cell |
| **N** | `SELECT ... UNION ALL` row | Inside the body of a SQL MERGE template (see below) |

**Column M is the recommended path.** Copy the M cells for your rows, paste into
`02-config-pyspark.py` (or any Python cell), and run. The formula handles all
Databricks-compatible escaping internally — backslash doubling and single-quote escaping
are applied in the correct order so `add_custom_query()` stores the expression correctly.

**Column N (SQL MERGE path)** produces rows for a bulk MERGE statement. The generated rows
must be embedded inside a `r"""..."""` Python raw string — **never** `f"""..."""`:

```python
# Correct: raw string for the SQL body, f-string only for catalog/schema names
tgt   = f"`{MY_CATALOG}`.`{DQ_SCHEMA}`.`configCustomQuery`"
query = (
    r"MERGE INTO " + tgt + r" AS t "
    r"USING (SELECT * FROM (VALUES "
    r"  (1, 'Schema.Table.Field', 'REGEX', '\\d+', ...) "   # ← paste N-column rows here
    r") AS s(...) WHERE _ID IS NOT NULL) ON t._ID = s._ID "
    r"WHEN MATCHED AND (...) THEN UPDATE SET ... "
    r"WHEN NOT MATCHED THEN INSERT ..."
)
spark.sql(query)
```

Why `r"""..."""`? Python f-strings process backslash sequences before Spark SQL ever
sees the string — `\'` becomes `'` silently, breaking `@InputValue` expressions with
`PARSE_SYNTAX_ERROR: Syntax error at or near '@'`. A raw string passes the SQL verbatim.

> If you paste N-column rows **directly into a SQL notebook cell** (not a Python string),
> replace every `''` inside expression values with `\'` — Databricks SQL legacy mode uses
> backslash escaping, not ANSI quote-doubling.

### Escaping rules (handled automatically by the formula)

| In your expression | Formula produces | Stored in table | Spark SQL sees |
|---|---|---|---|
| `\d+` (regex backslash) | `\\d+` | `\\d+` | `\d+` ✓ |
| `O'Brien` (quote) | `O\'Brien` (M) / `O''Brien` (N) | `O'Brien` | `O'Brien` ✓ |

You do **not** need to pre-escape anything before typing into the Excel cells. Fill in the
raw expression (e.g. `^\d{4}$`); the formula handles the rest.

---

## Known Limitations

| # | Area | Description |
|---|---|---|
| 1 | **Bug — `add_custom_query()`** | `CustomQueryDescription` column is `NOT NULL` in the DDL but the method passes `null` when no `description` argument is provided. Always supply a `description` when calling `add_custom_query()`, `add_custom_query_regex()`, or `add_custom_query_sql()` to avoid an INSERT constraint violation. |
| 2 | **Silent pattern skip** | If a `PatternName` in `configFieldAllowedPattern` does not match any row in `masterPattern`, the rule is silently ignored (equivalent to SQL's `ELSE NULL`). A typo in a pattern name will go undetected unless `verify_config()` is run. |
| 3 | **No transaction guarantees** | The assessment writes DQ columns, audit rows, and stat rows in three separate Spark actions. A cluster failure between steps can leave partial results. Re-run the assessment with `reset_eligible_flag=True` to recover. |
| 4 | **Date validation outside Databricks** | If `python-dateutil` is not installed, date/time pattern checks fall back to three hard-coded regex formats. This is rarely an issue on Databricks Runtime but may affect local unit test runs that exercise the date validators. |
| 5 | **No parameterised SQL** | Config is written via f-string SQL. Backslash and single-quote escaping uses Databricks legacy Spark SQL mode (`\` as escape character). Field names and expressions provided by trusted developers are safe; do not pass untrusted user input directly into ConfigManager methods. |

---

*Data Quality Non-Functional Assessment Framework — Databricks Edition*
*Generalised for open-source publication — no client-specific data included*
