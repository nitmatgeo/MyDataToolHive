# databricks-excel-ingest-framework

> Stop writing one-off scripts for every Excel file. Let the framework figure out the structure, map the columns, and hand you clean, confident results — ready for Delta Lake.

---

## The problem

Excel files from the real world are messy.

Headers span multiple rows. Cells are merged. Column names vary across teams, regions, and time — "Order No.", "Ord ID", "Transaction Number" all mean the same thing. Files arrive password-protected, with hidden columns, or with sections separated by blank columns.

When you're building a Databricks data pipeline that needs to ingest Excel files reliably — especially at scale, from multiple sources — this becomes a significant engineering problem every single time.

---

## What this framework does

**`databricks-excel-ingest-framework`** is a Python package that handles the entire Excel-to-Delta pipeline in four structured stages:

```
Validate  →  Detect Structure  →  Extract Metadata  →  Map to Canonical Fields
```

| Stage | What it does |
|-------|-------------|
| **Validate** | Checks the file exists, is a valid Excel format, is readable (password?), and lists all sheets |
| **Detect Structure** | Finds header rows automatically — even across multiple rows and merged cells. Detects blank separators, hidden columns, data boundaries |
| **Extract Metadata** | Builds hierarchical column names like `[Contact Info].[Email Address]`, groups columns into sections, and generates a SHA-256 signature so identical layouts are recognised instantly |
| **Map to Canonical** | Maps each column header to your field names using rule-based confidence scoring, optionally boosted by an LLM |

The result is a structured, confidence-scored mapping of every column — ready to route into your Delta tables, flag for human review, or cache for reuse.

---

## Why you need it

### Without this framework

You write a bespoke script for every file. It works until the source changes a column name, adds a merged header row, or sends a file from a different region with different terminology. Then it breaks silently — or loudly.

### With this framework

One consistent pipeline handles any Excel file. Structure is detected, not assumed. Column mapping is scored, not guessed. Columns that don't map confidently are flagged for review rather than silently misrouted. And when the same file layout arrives again, the SHA-256 signature means you don't re-process what you've already figured out.

---

## When to use it

Use this framework when:

- You're ingesting Excel files from **multiple sources or teams** that don't follow a consistent format
- Your files have **complex headers** — merged cells, multi-row headings, section separators
- You need to **map inconsistent column names** (from different regions, systems, or vendors) to a single canonical schema
- You're on **Databricks** and want native support for Unity Catalog Volumes, DBFS, and Azure Storage paths
- You want **auditability** — every mapping decision is scored and explained, not a black box
- You want **AI-assisted mapping** but with a human review gate for low-confidence columns

---

## Key features

**Excel handling**
- Auto-detects header rows — works with single-row, multi-row, and merged-cell headers
- Handles blank column separators (section detection)
- Identifies and flags hidden columns
- Supports `.xlsx`, `.xlsm`, `.xls`
- Password-protected file support

**Canonical mapping**
- Hybrid confidence scoring: 70% rule-based + 30% LLM (LLM is optional)
- Three-bucket output: `AUTO_APPROVED` (>0.9), `NEEDS_REVIEW` (0.7–0.9), `REQUIRES_HUMAN` (<0.7)
- Fully domain-agnostic — you supply the canonical dictionary, the framework supplies no assumptions
- Learns from previous mappings — pass prior results to boost confidence on already-seen headers
- SHA-256 header signature for instant reuse detection across identical file layouts

**Databricks-native**
- Recognises Unity Catalog Volume paths (`/Volumes/...`), DBFS, and Azure Storage (`abfss://`)
- Pluggable LLM adapters: Databricks Foundation Models, OpenAI, Anthropic — all optional
- Databricks Foundation Models adapter requires no endpoint setup — uses the pay-per-token API
- Results return as plain Python dicts — your pipeline writes to Delta, not the framework

**Developer experience**
- `framework.guide()` — prints a step-by-step usage guide at any time
- `framework.sample_usage(spark)` — extracts sample notebooks directly into your Databricks Workspace
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

framework = ExcelIngestFramework(spark=spark)

result = framework.ingest(
    file_path="/Volumes/my_catalog/my_schema/my_volume/data.xlsx",
    canonical_dict={
        "order_id":     ["order no", "ord id", "transaction id"],
        "product_name": ["product", "item name", "description"],
        "store_name":   ["store", "retail unit", "shop name"],
        "quantity":     ["qty", "units", "quantity"],
    },
)

for m in result.mappings:
    print(f"{m.mapping_status.value:<18} {m.final_confidence:.2f}  "
          f"{m.hierarchical_header}  →  {m.canonical_field or 'UNMAPPED'}")
```

**With an LLM adapter:**

```python
from excel_ingest.mapping.adapters.databricks import DatabricksAdapter

adapter = DatabricksAdapter(model="databricks-llama-3-70b-instruct")
framework = ExcelIngestFramework(spark=spark, adapter=adapter)
```

**Multi-sheet file** — always specify the sheet:

```python
from excel_ingest.structure import FileProcessingConfig

result = framework.ingest(
    file_path=...,
    canonical_dict=...,
    config=FileProcessingConfig(sheet_name="Employees"),
)
```

**Get oriented instantly:**

```python
framework.guide()               # step-by-step guide printed to stdout
framework.sample_usage(spark)   # sample notebooks → your Databricks Workspace
```

---

## Confidence scoring

Every mapped column gets a confidence score. No silent failures.

| Score | Status | Meaning |
|-------|--------|---------|
| > 0.9 | `AUTO_APPROVED` | Safe to load without review |
| 0.7 – 0.9 | `NEEDS_REVIEW` | Probable match — worth a human check |
| < 0.7 | `REQUIRES_HUMAN` | Low confidence — do not auto-load |
| — | `UNMAPPED` | No candidate found |

```python
from excel_ingest import MappingStatus

auto     = [m for m in result.mappings if m.mapping_status == MappingStatus.AUTO_APPROVED]
review   = [m for m in result.mappings if m.mapping_status == MappingStatus.NEEDS_REVIEW]
manual   = [m for m in result.mappings if m.mapping_status == MappingStatus.REQUIRES_HUMAN]
```

---

## Persist to Delta

The framework returns Python dicts — your pipeline decides where and when to write:

```python
spark.createDataFrame([result.file_record()]).write \
    .mode("append").saveAsTable("`catalog`.`schema`.`excel_file_metadata`")

spark.createDataFrame(result.metadata_records()).write \
    .mode("append").saveAsTable("`catalog`.`schema`.`excel_column_metadata`")

spark.createDataFrame(result.mapping_records()).write \
    .mode("append").saveAsTable("`catalog`.`schema`.`excel_canonical_mappings`")
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

Run `framework.sample_usage(spark)` to extract these into your Databricks Workspace:

| Notebook | Covers |
|----------|--------|
| `01-install.py` | Installation on cluster |
| `02-validate.py` | File validation |
| `03-structure.py` | Structure detection |
| `04-metadata.py` | Metadata extraction |
| `05-mapping.py` | Canonical mapping with all adapter options |

---

## Status

Pre-release alpha (`0.1.0ax`). API may change before `1.0.0`.

---

## Author

Nitin Mathew George · [github.com/NitMatGeo](https://github.com/NitMatGeo)
