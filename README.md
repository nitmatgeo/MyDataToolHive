# databricks-excel-ingest-framework

> Stop writing one-off scripts for every Excel file. Let the framework figure out the structure, map the columns, and hand you clean, confident results — ready for Delta Lake.

---

> [!NOTE]
> **Get started in 2 minutes**
>
> ```bash
> pip install databricks-excel-ingest-framework
> ```
>
> In a Databricks notebook:
>
> ```python
> %pip install "databricks-excel-ingest-framework[databricks]"
> dbutils.library.restartPython()
> ```
>
> Then extract the sample notebooks directly into your workspace — they walk through every stage with real files:
>
> ```python
> from excel_ingest import ExcelIngestFramework
> framework = ExcelIngestFramework(spark=spark)
> framework.sample_usage(spark)   # extracts 01-install through 05-load into your Workspace
> framework.guide()               # prints a step-by-step usage guide to stdout
> ```
>
> Run `01-install.py` first — it installs the package on the cluster, uploads sample Excel files to a Unity Catalog Volume, and sets shared variables that all subsequent notebooks inherit.

---

## The problem

Excel files from the real world are messy.

Headers span multiple rows. Cells are merged. Column names vary across teams, regions, and time — "Order No.", "Ord ID", "Transaction Number" all mean the same thing. Files arrive password-protected, with hidden columns, or with sections separated by blank columns.

When you're building a Databricks data pipeline that needs to ingest Excel files reliably — especially at scale, from multiple sources — this becomes a significant engineering problem every single time.

---

## What this framework does

**`databricks-excel-ingest-framework`** handles the entire Excel-to-Delta pipeline in five structured stages:

```
Validate  →  Detect Structure  →  Extract Metadata  →  Load  →  Map to Canonical (optional)
```

| Stage | What it does |
|-------|-------------|
| **Validate** | Checks the file exists, is a valid Excel format, is readable (password?), and lists all sheets |
| **Detect Structure** | Finds header rows automatically — even across multiple rows and merged cells. Detects blank separators, hidden columns, data boundaries |
| **Extract Metadata** | Builds hierarchical column names like `[Contact Info].[Email Address]`, groups columns into sections, generates SQL-safe Delta column names, and produces a SHA-256 signature so identical layouts are recognised instantly |
| **Load** | Reads data rows and returns a Spark DataFrame — all columns as `STRING`, ready for your bronze Delta table |
| **Map to Canonical** *(optional)* | Maps bronze column names to your business field names using rule-based confidence scoring, optionally boosted by an LLM |

---

## Why you need it

### Without this framework

You write a bespoke script for every file. It works until the source changes a column name, adds a merged header row, or sends a file from a different region with different terminology. Then it breaks silently — or loudly. And you have no record of what columns were in the file or how confident any mapping was.

### With this framework

One consistent pipeline handles any Excel file. Structure is detected, not assumed. Column names are made SQL-safe and deterministic. A SHA-256 signature means you instantly know when a file's layout has changed — before you overwrite clean data with a mismatched schema. And when the same layout arrives again, you skip re-processing entirely.

---

## When to use it

Use this framework when:

- You're ingesting Excel files from **multiple sources or teams** that don't follow a consistent format
- Your files have **complex headers** — merged cells, multi-row headings, section separators
- You need to **map inconsistent column names** (from different regions, systems, or vendors) to a single canonical schema
- You're on **Databricks** and want native support for Unity Catalog Volumes, DBFS, and Azure Storage paths
- You want **schema drift detection** — know immediately when a supplier changes their file format
- You want **auditability** — every mapping decision is scored and explained, not a black box

---

## Pipeline stages — detailed guide

Each stage builds on the previous one and can be run individually or in bulk across many files.

The framework processes each Excel file through four stages. Each stage builds on the previous one and can be run individually or in bulk across many files.

```
Stage 1          Stage 2              Stage 3               Stage 4 (optional)
Validate    →   Detect Structure  →  Extract Metadata  →   Map to Canonical Fields
                                          ↓
                                         Load  (Stage 5)
```

### Stage 1 — Validate

**What it does:** Confirms the file exists, is a valid Excel format, can be opened (password check), and lists every sheet.

| Output field | What it tells you |
|---|---|
| `status` | `PASSED` / `WARNING` / `FAILED` |
| `status_description` | Plain-English explanation — no docs lookup needed |
| `is_readable` | `False` means the pipeline cannot proceed |
| `is_password_protected` | `True` means a password is required at every subsequent stage |
| `visible_sheets` | Sheets the framework can process — use to pick the right one |
| `warnings` | Non-blocking issues (e.g. hidden sheets present) — review, not a hard stop |
| `errors` | Blocking issues — must be resolved before continuing |

**ETL guidance:**
- Run this first — always, for every file in a batch.
- `status == PASSED` → proceed to Structure.
- `status == WARNING` → proceed, but log the warning. Hidden sheets are common and usually not a problem — but sometimes they contain the data you actually need.
- `status == FAILED` → stop this file. Whether you fail the whole batch or just skip this one file is your pipeline's decision. Common pattern: fail individually, log the error, let the rest continue.
- In bulk mode, collect all validation records first (`r.summary_record()`) then filter `status != PASSED` to build your quarantine list before starting Structure.

```python
results = [framework.validate(path, password=pw) for path, pw in files]
failed  = [r for r in results if r.status.value == "FAILED"]
# quarantine failed, continue with the rest
```

---

### Stage 2 — Detect Structure

**What it does:** Opens the specified sheet and works out where headers are, where data starts, which columns are blank separators, which are hidden, and whether the file can be processed as-is.

| Output field | What it tells you |
|---|---|
| `status` / `status_description` | `OK`, `NO_DATA`, `SHEET_NOT_SPECIFIED`, `HEADER_DETECTION_FAILED`, `UNREADABLE` |
| `is_actionable` | **`True` = pipeline must stop** — see below |
| `header_rows` | 1-based row numbers detected as headers (e.g. `[1, 2]` for two-row headers) |
| `data_row_count` | How many data rows were found (0 = headers only) |
| `header_range` / `data_range` | Excel cell ranges, e.g. `A1:Z2` / `A3:Z1500` |
| `blank_columns` | Column indices used as section separators |
| `hidden_columns` | Column indices that are hidden in the workbook |

**`is_actionable = True` means: stop, fix the config, re-run.** Specifically:

| Status | Root cause | Fix |
|---|---|---|
| `SHEET_NOT_SPECIFIED` | File has multiple visible sheets and no `sheet_name` was provided | Add `FileProcessingConfig(sheet_name="Sheet Name")` |
| `HEADER_DETECTION_FAILED` | Framework couldn't determine where headers end and data begins | Add `FileProcessingConfig(static_header_rows=[1, 2])` |
| `UNREADABLE` | File couldn't be opened at this stage | Re-validate; check password |

`is_actionable = False` with `status = NO_DATA` is not an error — the file has valid headers but zero data rows. You can still extract metadata and build the schema.

**ETL guidance:**
- Reuse the same `FileProcessingConfig` for all subsequent stages — it carries `sheet_name`, `static_header_rows`, `data_start_row`, `ignore_rows`, and `ignore_row_ranges` through the entire pipeline.
- Store per-file configs in a Delta override table and load them with `FileProcessingConfig.from_override(row)`. This avoids hardcoding configs for every file in your pipeline code.
- In bulk mode: filter `is_actionable = True` after the structure loop — alert the team, they update the config, and re-run only the affected files.

```python
configs = spark.table("my_catalog.bronze.excel_file_configs").collect()

structures = []
for row in configs:
    config = FileProcessingConfig.from_override(row)
    s = framework.detect_structure(row["file_path"], config=config, password=row["password"])
    structures.append((row["file_id"], s))

# Pause on actionable files — do not proceed to metadata
actionable = [(fid, s) for fid, s in structures if s.is_actionable]
```

---

### Stage 3 — Extract Metadata

**What it does:** Reads the header rows detected in Stage 2 and builds a complete column inventory — hierarchical labels, SQL-safe Delta column names, section groupings, and a SHA-256 layout signature.

| Output field | What it tells you |
|---|---|
| `hierarchical_header` | The Excel column label as read — e.g. `[Cost & Margin].[Margin %]` |
| `db_canonical_bronze_column_name` | SQL-safe Delta column name — e.g. `cost_and_margin__margin_pct` |
| `column_group` | Section number — columns separated by blank dividers get different group IDs |
| `header_signature` | SHA-256 of all column headers — identical across files with the same layout |

**Name derivation rules:**
- Leaf header used when it is unique across the sheet → `customer_name`
- Full path (double-underscore separator) used when the leaf is duplicated → `section_1__customer_name`
- `&` → `and` | `%` → `pct` | spaces / special chars → `_`

**`header_signature` — schema drift detection:**

The SHA-256 signature is computed from the ordered list of all column headers. Store it after every ingest run and compare on the next run:
- Same signature = same layout → reuse previous column mapping, no re-processing needed
- Different signature = headers changed (added, removed, renamed, or reordered) → trigger a fresh mapping and alert the team

```python
# Persist after each run
spark.createDataFrame([meta.signature_record()]) \
    .write.mode("append").saveAsTable("bronze.excel_schema_signatures")

# On next run: compare
new_sig    = framework.extract_metadata(path, structure).file_metadata.header_signature
stored_sig = spark.table("bronze.excel_schema_signatures") \
                  .filter(f"file_id = '{file_id}'").orderBy("run_date", ascending=False) \
                  .first()["header_signature"]

if new_sig != stored_sig:
    # Schema changed — re-map columns, alert team
    ...
```

**Consuming the metadata:**

- `meta.bronze_schema()` → `{db_canonical_bronze_column_name: column_index}` — use this to build a per-file `SELECT` for the bronze load
- `build_superset_schema(all_metadata)` → sorted union of all column names across multiple files — use this to generate a single bronze DDL that covers every file
- `meta.column_records()` → flat list of dicts, ready for `spark.createDataFrame()` — persist to a Delta reference table for your mapping team

```python
# Generate bronze DDL for a superset table (covers all files)
from excel_ingest import build_superset_schema

all_cols = build_superset_schema(all_metadata)
col_defs = ",\n    ".join(f"`{c}` STRING" for c in all_cols)
ddl = f"CREATE TABLE IF NOT EXISTS bronze.excel_data ({col_defs}, source_file STRING, source_sheet STRING)"
spark.sql(ddl)
```

---

### Stage 4 (optional) — Map to Canonical Fields

**What it does:** Maps each `db_canonical_bronze_column_name` to a business field name you define — using rule-based scoring, optionally boosted by an LLM.

This is a **silver-layer concern** — it answers "which bronze column maps to which business field?" Useful when different files use different headers for the same concept (e.g. `order_no`, `ord_id`, `transaction_number` all mean `order_id`).

See `06.BETA-mapping.py` in the sample notebooks. Requires `canonical_dict` supplied by the caller — the framework has no domain knowledge baked in.

---

### Stage 5 — Load

**What it does:** Reads the actual data rows from the Excel file and returns a Spark DataFrame with bronze column names. No casting — all columns are `STRING`. Cast to correct types in the silver layer.

| Auto-column | Value |
|---|---|
| `source_file` | File path |
| `source_sheet` | Sheet name |
| `insert_timestamp` | UTC timestamp at load time |

**Options:**
- `skip_hidden_columns=True` — exclude columns flagged as hidden in Stage 2 (default `False` — hidden columns often contain real data)
- `config` — pass `ignore_rows` or `ignore_row_ranges` to skip specific rows at load time (e.g. subtotal rows)

**Combining multiple files:**

`framework.combine(results)` unions multiple `LoadResult` DataFrames into one. Files that don't have a particular column get `NULL` for that column — no data is lost. Auto-columns are always at the end.

```python
# Single file
result = framework.load(path, structure, metadata, password=pw)
result.df.write.mode("append").saveAsTable("bronze.sales_data")

# All sheets in a multi-sheet file
from excel_ingest.structure import FileProcessingConfig

results = []
for sheet in framework.validate(path).visible_sheet_names:
    config    = FileProcessingConfig(sheet_name=sheet)
    structure = framework.detect_structure(path, config=config)
    metadata  = framework.extract_metadata(path, structure, file_id=sheet)
    results.append(framework.load(path, structure, metadata))

combined = framework.combine(results)   # one DataFrame, all sheets
combined.write.mode("append").saveAsTable("bronze.all_regions")
```

---

## End-to-end ETL flow

A typical production pattern for a batch of files:

```
1. Validate all files (bulk)
      → Quarantine FAILED files, log errors, alert team
      → Proceed with PASSED + WARNING

2. Detect Structure for each file (bulk)
      → Quarantine is_actionable=True files, alert team to fix FileProcessingConfig
      → Proceed with is_actionable=False

3. Extract Metadata for each file
      → Compare header_signature against stored signatures
      → If changed: re-map and alert; if same: reuse cached column mapping
      → Persist column metadata to reference table

4. Load each file
      → framework.load() → result.df (all columns STRING)
      → framework.combine() for files sharing a schema
      → Write to bronze Delta table(s)

5. Silver layer (outside this framework)
      → Cast STRING columns to correct types
      → Apply canonical field mapping (Stage 4) if needed
      → Join / enrich / validate business rules
```

**Config management pattern — avoid hardcoding per-file settings:**

```python
# Store per-file config in Delta
# Columns: file_path, file_id, password, sheet_name, static_header_rows, ...

configs = spark.table("bronze.excel_file_configs").collect()

for row in configs:
    config    = FileProcessingConfig.from_override(row)
    structure = framework.detect_structure(row["file_path"], config=config, password=row["password"])
    metadata  = framework.extract_metadata(row["file_path"], structure, file_id=row["file_id"])
    result    = framework.load(row["file_path"], structure, metadata, password=row["password"])
    result.df.write.mode("append").saveAsTable(row["target_table"])
```

---

## Key features

**Excel handling**
- Auto-detects header rows — works with single-row, multi-row, and merged-cell headers
- Handles blank column separators (section detection with `column_group` IDs)
- Identifies and flags hidden columns (included by default, exclusion is opt-in)
- Supports `.xlsx`, `.xlsm`, `.xls`
- Password-protected file support (AES encryption via `msoffcrypto-tool`)

**Schema management**
- SHA-256 header signature for instant schema drift detection across runs
- `db_canonical_bronze_column_name` — deterministic, SQL-safe Delta column names derived from Excel headers
- `build_superset_schema()` — union of all column names across a file batch for consolidated bronze tables
- `COLUMN_RECORD_FIELDS`, `STRUCTURE_RECORD_FIELDS`, `VALIDATION_RECORD_FIELDS` — framework-exported field-order constants for consistent `display()` output in notebooks

**Loading**
- `framework.load()` → `LoadResult` with `result.df` (Spark DataFrame, all columns `STRING`)
- `framework.combine()` → unions multiple `LoadResult`s with NULL-fill for missing columns
- `FileProcessingConfig.from_override(row)` → build config from Delta table row for config-driven pipelines

**Canonical mapping (optional)**
- Hybrid confidence scoring: 70% rule-based + 30% LLM (LLM is optional)
- Three-bucket output: `AUTO_APPROVED` (>0.9), `NEEDS_REVIEW` (0.7–0.9), `REQUIRES_HUMAN` (<0.7)
- Fully domain-agnostic — you supply the canonical dictionary
- Pluggable LLM adapters: Databricks Foundation Models, OpenAI, Anthropic

**Databricks-native**
- Recognises Unity Catalog Volume paths (`/Volumes/...`), DBFS, and Azure Storage (`abfss://`)
- Results return as plain Python dicts / Spark DataFrames — your pipeline writes to Delta, not the framework
- Works outside Databricks too — core pipeline needs only `openpyxl`

---

## Install

```bash
# Core only (no LLM)
pip install databricks-excel-ingest-framework

# With Databricks Foundation Models
pip install "databricks-excel-ingest-framework[databricks]"

# With OpenAI or Anthropic
pip install "databricks-excel-ingest-framework[openai]"
pip install "databricks-excel-ingest-framework[anthropic]"

# All adapters
pip install "databricks-excel-ingest-framework[all]"
```

In a Databricks notebook:

```python
%pip install "databricks-excel-ingest-framework[databricks]"
dbutils.library.restartPython()
```

---

## Quick start

```python
from excel_ingest import ExcelIngestFramework
from excel_ingest.structure import FileProcessingConfig

framework = ExcelIngestFramework(spark=spark)

# Stage by stage
path      = "/Volumes/my_catalog/bronze/my_volume/data.xlsx"
config    = FileProcessingConfig(sheet_name="Sales")   # omit for single-sheet files

valid     = framework.validate(path)
structure = framework.detect_structure(path, config=config)
metadata  = framework.extract_metadata(path, structure, file_id="SALES_UK")
result    = framework.load(path, structure, metadata)

result.df.display()   # Spark DataFrame — all columns STRING + source_file, source_sheet, insert_timestamp
```

**Multi-sheet combine:**

```python
results = []
for sheet in framework.validate(path).visible_sheet_names:
    cfg  = FileProcessingConfig(sheet_name=sheet)
    s    = framework.detect_structure(path, config=cfg)
    m    = framework.extract_metadata(path, s)
    results.append(framework.load(path, s, m))

combined = framework.combine(results)
```

**Full pipeline with LLM mapping:**

```python
from excel_ingest.mapping.adapters.databricks import DatabricksAdapter

adapter  = DatabricksAdapter(model="databricks-llama-3-70b-instruct")
framework = ExcelIngestFramework(spark=spark, adapter=adapter)

result = framework.ingest(
    file_path=path,
    canonical_dict={
        "order_id":     ["order no", "ord id", "transaction id"],
        "product_name": ["product", "item name", "description"],
    },
)
for m in result.mappings:
    print(f"{m.mapping_status.value:<18} {m.final_confidence:.2f}  {m.hierarchical_header}  →  {m.canonical_field or 'UNMAPPED'}")
```

---

## LLM adapters

All adapters are optional. The framework runs rule-only by default.

| Adapter | Install extra | Default model |
|---------|--------------|---------------|
| `DatabricksAdapter` | `[databricks]` | `databricks-llama-3-70b-instruct` |
| `OpenAIAdapter` | `[openai]` | `gpt-4o-mini` |
| `AnthropicAdapter` | `[anthropic]` | `claude-haiku-4-5-20251001` |

All model names are constructor parameters — swap to any model the provider supports.

> **Privacy note:** Only column header names and your canonical dictionary keys are sent to the LLM. No cell values or data are transmitted.

---

## Supported path formats

| Path | Location |
|------|---------|
| `/Volumes/catalog/schema/volume/file.xlsx` | Unity Catalog Volume |
| `/dbfs/...` or `dbfs:/...` | DBFS |
| `abfss://container@account.dfs.core.windows.net/...` | Azure Data Lake |
| `/Workspace/...` | Databricks Workspace |
| `/tmp/...` or local path | Local filesystem |

---

## Sample notebooks

Run `framework.sample_usage(spark)` to extract these into your Databricks Workspace, or find them at `excel_ingest/sample_usage/` in the package source.

| Notebook | Covers |
|----------|--------|
| `01-install.py` | Cluster install, Volume setup, sample file upload |
| `02-validate.py` | Stage 1 — file validation, negative examples |
| `03-structure.py` | Stage 2 — structure detection with `FileProcessingConfig` |
| `04-metadata.py` | Stage 3 — metadata extraction, signatures, bronze DDL |
| `05-load.py` | Stage 5 — load patterns: single sheet, multi-sheet, combine, override configs |
| `06.BETA-mapping.py` | Stage 4 (optional) — canonical field mapping with LLM adapters |

> Start with `01-install.py`. It uploads sample files covering 12 different Excel complexity scenarios (hidden sheets, password-protected, 3-level merged headers, blank column separators, etc.) and sets the `VOLUME_PATH` variable inherited by all other notebooks.

---

## Status

Pre-release alpha (`0.1.0a20`). API may change before `1.0.0`.

---

## Author

Nitin Mathew George · [github.com/NitMatGeo](https://github.com/NitMatGeo)
