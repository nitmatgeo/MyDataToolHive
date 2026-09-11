---
applyTo: "databricks-excel-ingest-framework/**"
---

# Excel Ingest Framework — Copilot Instructions

You are working on the **Excel Ingest Framework** in `databricks-excel-ingest-framework/`.

Start by reading `databricks-excel-ingest-framework/CLAUDE.md` — it contains the full architecture, naming conventions, and design principles.

---

## Framework identity

| Detail | Value |
|--------|-------|
| PyPI name | `databricks-excel-ingest-framework` |
| Import name | `excel_ingest` |
| Main class | `ExcelIngestFramework` |
| Version | read from `pyproject.toml` — do not hardcode |
| Package folder | `databricks-excel-ingest-framework/excel_ingest/` |

---

## Architecture

```
ExcelIngestFramework(spark=None, adapter=None)
    ├── .validate(file_path, password)
    │       → FileValidationResult  [validation.py]
    │         .status: PASSED | WARNING | FAILED
    │         .visible_sheet_names, .all_sheet_names
    │         .is_password_protected, .location_type
    │
    ├── .detect_structure(file_path, config, password)
    │       → FileStructureMetadata  [structure.py]
    │         .status: VALID | NO_HEADERS | NO_DATA | EMPTY_FILE | INVALID_STRUCTURE | SHEET_NOT_SPECIFIED
    │         .header_range   → "A1:L1"   (Excel A1 notation)
    │         .data_range     → "A2:L21"  (Excel A1 notation)
    │         .merged_cells, .blank_column_indices, .hidden_column_indices
    │
    ├── .extract_metadata(file_path, structure=None, file_id=None, password=None, config=None)
    │       → MetadataExtractionResult  [metadata.py]
    │         If structure is not supplied, detect_structure() is called automatically
    │         (password and config are forwarded to that auto-detect call only).
    │         .file_metadata.header_signature  (SHA-256 of all column headers)
    │         .column_metadata[n]:
    │             .hierarchical_header          "[Parent].[Child]" or "[Header]"
    │             .db_canonical_bronze_column_name  SQL-safe Delta column name
    │             .section_id, .column_letter, .column_index
    │             .is_blank_column, .is_hidden_column, .is_part_of_merge
    │         .column_records()      → List[Dict] — human-readable, for display()
    │         .to_delta_records()    → List[Dict] — for Delta persistence
    │         .signature_record()    → Dict — file_id, file_name, sheet_name, header_signature
    │         .bronze_schema()       → Dict[col_name → col_index] for non-blank columns only
    │
    ├── .map_to_canonical(metadata, canonical_dict, country_code=None, prior_mappings=None,
    │                     adapter=None, skip_blank_columns=True)
    │       → List[CanonicalMapping]  [mapping/engine.py]
    │         .canonical_field          ← silver target
    │         .mapping_status           AUTO_APPROVED | NEEDS_REVIEW | REQUIRES_HUMAN | UNMAPPED
    │         .final_confidence, .rule_score, .llm_confidence, .llm_reasoning
    │         .to_dict()    includes status_description + requires_action as flat fields
    │
    ├── .load(file_path, structure, metadata, password, config, skip_hidden_columns)
    │       → LoadResult  [loader.py]
    │         .df              Spark DataFrame — all bronze columns (STRING) + auto-columns
    │         Auto-columns: source_file, source_sheet, insert_timestamp
    │
    ├── .combine(results)   ← list of LoadResult
    │       → pyspark.sql.DataFrame  (union, NULL-filled)
    │
    ├── .guide()            → None  [step-by-step usage printed to stdout]
    ├── .sample_usage(spark)→ str   [extracts bundled notebooks to /Workspace/Users/{you}/...]
    │
    └── .ingest(file_path, canonical_dict, config=None, password=None, file_id=None,
                country_code=None, prior_mappings=None, adapter=None,
                skip_blank_columns=True)
            → IngestResult
              .success (bool), .errors (List[str])
              .summary_record()    → Dict — file_id, total_cols, auto_approved,
                                            needs_review, requires_human, unmapped
              .mapping_records()   → List[Dict] — per-column detail
              .metadata_records()  → List[Dict] — column metadata for Delta
              .file_record()       → Optional[Dict] — None if metadata failed

FileProcessingConfig  [structure.py]
    .from_override(dict_or_spark_row)   ← classmethod, builds from dict or Spark Row

Module-level exports (all importable directly from `excel_ingest`):
    ExcelIngestFramework, IngestResult
    FileValidationResult, ValidationStatus, VALIDATION_RECORD_FIELDS
    FileStructureMetadata, FileProcessingConfig, FileStatus, STRUCTURE_RECORD_FIELDS
    MetadataExtractionResult, combine_column_records, build_superset_schema
    COLUMN_RECORD_FIELDS, SIGNATURE_RECORD_FIELDS
    LoadResult
    map_to_canonical, CanonicalMapping, MappingStatus, MappingMethod

LLM adapters (all optional, gated behind extras):
    DatabricksAdapter(model="databricks-llama-3-70b-instruct", host=None, token=None)
    OpenAIAdapter(model="gpt-4o-mini", api_key=None)
    AnthropicAdapter(model="claude-haiku-4-5-20251001", api_key=None)

Confidence scoring:
    final = 0.7 * rule_score + 0.3 * llm_confidence   (when adapter present)
    final = rule_score                                   (adapter=None)
    > 0.9 → AUTO_APPROVED  |  0.7–0.9 → NEEDS_REVIEW  |  < 0.7 → REQUIRES_HUMAN
```

---

## Bronze vs Silver — critical distinction

**Bronze** = as-is file structure loaded to Delta. Column names from `db_canonical_bronze_column_name` (SQL-safe, derived from Excel header).

**Silver** = business schema. Column names from `canonical_field` (caller's `canonical_dict`). Renaming, unpivoting, merging happen here — **out of scope for this framework**.

`db_canonical_bronze_column_name` naming rules:
- `&` → `and`  |  `%` → `pct`  |  all other non-alphanumeric → `_`
- Hierarchy levels joined with `__` (double underscore) — not dot
- Examples: `[Customer Name]` → `customer_name` | `[Cost & Margin].[Margin %]` → `cost_and_margin__margin_pct`

---

## Common patterns

### Full pipeline

```python
from excel_ingest import ExcelIngestFramework

framework = ExcelIngestFramework(spark=spark)

result = framework.ingest(
    file_path="/Volumes/<catalog>/<schema>/<volume>/data.xlsx",
    canonical_dict={
        "order_id":     ["order id", "order no", "transaction id"],
        "product_name": ["product name", "item name", "description"],
    },
    country_code="UK",
    file_id="ORDERS_2026_Q1",
    # config=FileProcessingConfig(sheet_name="Orders"),  # MANDATORY for multi-sheet files
)

display(spark.createDataFrame([result.summary_record()]))
display(spark.createDataFrame(result.mapping_records()))
```

### Act on mapping results — no MappingStatus import needed in notebooks

```python
df = spark.createDataFrame(result.mapping_records())
df.filter("requires_action = true").display()       # needs human attention
df.filter("mapping_status = 'AUTO_APPROVED'").display()
df.filter("mapping_status = 'UNMAPPED'").display()  # add to canonical_dict
```

### Multi-sheet file — sheet_name is MANDATORY

```python
from excel_ingest.structure import FileProcessingConfig
result = framework.ingest(
    file_path=..., canonical_dict=...,
    config=FileProcessingConfig(sheet_name="Orders"),
)
```

### Load data rows as Spark DataFrame (Stage 5)

```python
structure = framework.detect_structure(path, config=config)
meta      = framework.extract_metadata(path, structure, file_id="S01")
result    = framework.load(path, structure, meta)
result.df.write.mode("append").saveAsTable("bronze.sales")
```

### Multi-file consolidation

```python
from excel_ingest import build_superset_schema

all_metadata = []
for file_path in my_files:
    structure = framework.detect_structure(file_path, config=...)
    meta      = framework.extract_metadata(file_path, structure, file_id=...)
    all_metadata.append(meta)

all_cols = build_superset_schema(all_metadata)
col_defs  = ",\n    ".join(f"`{c}` STRING" for c in all_cols)
spark.sql(f"CREATE TABLE IF NOT EXISTS <catalog>.<schema>.bronze ({col_defs}, source_file STRING, source_sheet STRING)")
```

### Persist to Delta

```python
spark.createDataFrame([result.file_record()]).write \
    .mode("append").saveAsTable("<catalog>.<schema>.excel_file_metadata")
spark.createDataFrame(result.metadata_records()).write \
    .mode("append").saveAsTable("<catalog>.<schema>.excel_column_metadata")
spark.createDataFrame(result.mapping_records()).write \
    .mode("append").saveAsTable("<catalog>.<schema>.excel_canonical_mappings")
```

### Install

```python
%pip install databricks-excel-ingest-framework --upgrade
%pip install "databricks-excel-ingest-framework[databricks]" --upgrade
%pip install "databricks-excel-ingest-framework[all]" --upgrade
dbutils.library.restartPython()
```

---

## Rules — always apply

- **Flat package layout** — `excel_ingest/` is at root of the subproject, not under `src/`.
- **`pyproject.toml` only** — no `setup.py`. Build backend: `setuptools.build_meta`.
- **No hardcoded canonical fields** — the canonical dictionary is 100% caller-supplied.
- **LLM adapters are always optional** — gated behind `try/except ImportError`. Core pipeline must work with openpyxl only.
- **`sheet_name` is MANDATORY for multi-sheet files** — flag this immediately.
- **`skip_blank_columns=True` by default** in both `map_to_canonical()` and `ingest()` — blank separator columns are excluded from mapping. Pass `False` to include them.
- **No Delta writes inside the package** — `.ingest()` returns dicts; caller writes to Delta.
- **Never pin versions in docs or notebooks** — use `--upgrade` only.
- **Version lives only in `pyproject.toml`**.
- **No `password` parameter on `extract_metadata()`** — Stage 3 reads from the `structure` object already in memory.
- **`__` (double underscore) as hierarchy separator** in `db_canonical_bronze_column_name` — not dot.
- **`requires_action` and string `mapping_status` in notebooks** — never import `MappingStatus` in sample notebooks; use DataFrame column filters only.
- **`MappingStatus` import only when extending the framework** — adapters, engine, tests.
- **Confidence formula is fixed**: `0.7 * rule + 0.3 * llm` — do not change weights.
- **`<catalog>` / `<schema>` / `<volume>` placeholders** in all SQL and paths.

---

## What to update when making changes

| Change type | Files to update |
|---|---|
| New public method or field | `__init__.py` exports + `__all__` |
| Any user-facing output column rename | `column_records()`, `to_delta_records()`, `to_dict()`, notebooks, CHANGELOG |
| New adapter | `adapters/<name>.py`, `pyproject.toml` extras, `__init__.py` if exported |
| Version bump | `pyproject.toml`, `CLAUDE.md` (version line + PyPI details), `CHANGELOG.md` |

---

## Response format

- Lead with the code block — explanation after, brief.
- Use placeholders (`<catalog>`, `<schema>`) unless the user has supplied real values.
- When showing output, always use `display(spark.createDataFrame(...))` — never bare `print()` for tabular data.
- Never import `MappingStatus` in notebook code shown to users — use `requires_action` filter.
- Sample domain is **FreshMart retail** (orders, products, stores) — not HR/employee.
