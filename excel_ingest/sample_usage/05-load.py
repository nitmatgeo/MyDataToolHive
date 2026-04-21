# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Load: Read Excel Data as Spark DataFrames
# MAGIC
# MAGIC `framework.load()` reads the actual data rows from an Excel sheet and returns
# MAGIC a **Spark DataFrame** with bronze column names ready for Delta Lake.
# MAGIC
# MAGIC **What every `result.df` always contains:**
# MAGIC
# MAGIC | Column type | Details |
# MAGIC |---|---|
# MAGIC | Bronze data columns | One per loadable Excel column — named using `db_canonical_bronze_column_name`, all `STRING` |
# MAGIC | `source_file` | Filename only (not full path) |
# MAGIC | `source_sheet` | Sheet name |
# MAGIC | `insert_timestamp` | Datetime when `load()` was called |
# MAGIC
# MAGIC **All values land as `STRING`** — cast to correct types in your silver layer.
# MAGIC
# MAGIC **Four load patterns covered in this notebook:**
# MAGIC
# MAGIC | Pattern | When to use |
# MAGIC |---|---|
# MAGIC | Single file, single sheet | Most common — one sheet per call |
# MAGIC | Single file, all sheets | Iterate visible sheets; keep each as separate DataFrame or table |
# MAGIC | Multiple files, keep separate | Files with different schemas → each to its own table |
# MAGIC | Multiple files, combine | Files with similar schemas → one consolidated DataFrame via `framework.combine()` |

# COMMAND ----------

# DBTITLE 1,Install & Inherit Variables
%run ./01-install

# COMMAND ----------

# DBTITLE 1,Config
MY_CATALOG    = "sampledatacatalog"
INGEST_SCHEMA = "bronze"
VOLUME_NAME   = "excel_ingest_samples"
VOLUME_PATH   = f"/Volumes/{MY_CATALOG}/{INGEST_SCHEMA}/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Initialise Framework
from excel_ingest import ExcelIngestFramework, LoadResult
from excel_ingest.structure import FileProcessingConfig

framework = ExcelIngestFramework(spark=spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 1 — Single File, Single Sheet
# MAGIC The simplest case: one file, one sheet, one DataFrame.

# COMMAND ----------

# DBTITLE 1,Pattern 1 — Load Single Sheet
path      = f"{VOLUME_PATH}/s01_simple_single_sheet.xlsx"
structure = framework.detect_structure(path)
meta      = framework.extract_metadata(path, structure, file_id="S01")
result    = framework.load(path, structure, meta)

print(f"Rows loaded : {result.df.count()}")
print(f"Columns     : {len(result.df.columns)}")
print(f"Source file : {result.source_file}")
print(f"Source sheet: {result.source_sheet}")

display(result.df)

# COMMAND ----------

# DBTITLE 1,Pattern 1 — Schema (bronze names + auto-columns at end)
result.df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 2 — Single File, All Visible Sheets
# MAGIC
# MAGIC `framework.validate()` gives you the sheet list — no hardcoding needed.
# MAGIC Loop over sheets and call `load()` once per sheet.
# MAGIC
# MAGIC - **Keep separate DataFrames** (or write each to its own table) when sheets have different structures.
# MAGIC - **Combine** (Pattern 4) when sheets share the same column layout.
# MAGIC
# MAGIC S06 has 4 regional sheets (UK, US, DE, AU) with identical structure — good candidate for combine.
# MAGIC S05 has 3 sheets with different structures — keep them separate.

# COMMAND ----------

# DBTITLE 1,Pattern 2 — Iterate All Sheets of S06 (same structure → separate DataFrames)
path   = f"{VOLUME_PATH}/s06_multi_sheet_same_structure.xlsx"
sheets = framework.validate(path).visible_sheet_names
print(f"Visible sheets: {sheets}")

sheet_results: dict[str, LoadResult] = {}

for sheet in sheets:
    config    = FileProcessingConfig(sheet_name=sheet)
    structure = framework.detect_structure(path, config=config)
    meta      = framework.extract_metadata(path, structure, file_id="S06")
    result    = framework.load(path, structure, meta)
    sheet_results[sheet] = result
    print(f"  {sheet}: {result.df.count()} rows, {len(result.df.columns)} columns")

# COMMAND ----------

# DBTITLE 1,Pattern 2 — View one sheet's data
# source_sheet column tells you which sheet each row came from
display(sheet_results["UK"].df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 3 — Multiple Files, Keep Separate
# MAGIC
# MAGIC When files have **different schemas**, each file loads into its own DataFrame (and its own table).
# MAGIC The `source_file` and `source_sheet` columns track origin.

# COMMAND ----------

# DBTITLE 1,Pattern 3 — Load Multiple Files Separately
SEPARATE_FILE_CONFIGS = [
    {"file": "s01_simple_single_sheet.xlsx",        "id": "S01", "password": None, "config": FileProcessingConfig()},
    {"file": "s05_multi_sheet_diff_structure.xlsx", "id": "S05", "password": None, "config": FileProcessingConfig(sheet_name="Orders")},
    {"file": "s09_hidden_columns.xlsx",             "id": "S09", "password": None, "config": FileProcessingConfig()},
    {"file": "s11_password_protected.xlsx",         "id": "S11", "password": "Password1234", "config": FileProcessingConfig()},
]

file_results: dict[str, LoadResult] = {}

for cfg in SEPARATE_FILE_CONFIGS:
    path      = f"{VOLUME_PATH}/{cfg['file']}"
    structure = framework.detect_structure(path, config=cfg["config"], password=cfg["password"])
    meta      = framework.extract_metadata(path, structure, file_id=cfg["id"])
    result    = framework.load(path, structure, meta, password=cfg["password"])
    file_results[cfg["id"]] = result
    print(f"  {cfg['id']}  {cfg['file']}: {result.df.count()} rows, {len(result.df.columns)} cols")

# COMMAND ----------

# DBTITLE 1,Pattern 3 — View each file's DataFrame separately
display(file_results["S01"].df)

# COMMAND ----------

display(file_results["S05"].df)

# COMMAND ----------

# DBTITLE 1,Pattern 3 — Exclude hidden columns (opt-in)
# S09 has hidden columns — by default load() includes them.
# Set skip_hidden_columns=True when hidden columns are intentionally suppressed.

path      = f"{VOLUME_PATH}/s09_hidden_columns.xlsx"
structure = framework.detect_structure(path)
meta      = framework.extract_metadata(path, structure, file_id="S09")

result_with_hidden    = framework.load(path, structure, meta)
result_without_hidden = framework.load(path, structure, meta, skip_hidden_columns=True)

print(f"With hidden columns   : {len(result_with_hidden.df.columns)} columns")
print(f"Without hidden columns: {len(result_without_hidden.df.columns)} columns")

display(result_with_hidden.df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern 4 — Multiple Files, Combine into One DataFrame
# MAGIC
# MAGIC `framework.combine(results)` takes a list of `LoadResult` and produces a single DataFrame:
# MAGIC - All bronze column names from all files are unioned (sorted alphabetically)
# MAGIC - Files missing a column get `NULL` for that column
# MAGIC - `source_file` / `source_sheet` / `insert_timestamp` are always at the end
# MAGIC
# MAGIC **Use this when files share a meaningful column overlap** — e.g. the same report
# MAGIC format sent by 10 regional offices, or the same sheet across multiple months.
# MAGIC
# MAGIC > Note: `combine()` performs no compatibility check. If you accidentally combine
# MAGIC > completely unrelated file types the result is valid but very sparse.
# MAGIC > Caller is responsible for combining only files that share a meaningful schema.

# COMMAND ----------

# DBTITLE 1,Pattern 4 — Combine All Regional Sheets of S06 into One DataFrame
REGIONAL_CONFIGS = [
    {"file": "s06_multi_sheet_same_structure.xlsx", "id": "S06_UK", "config": FileProcessingConfig(sheet_name="UK")},
    {"file": "s06_multi_sheet_same_structure.xlsx", "id": "S06_US", "config": FileProcessingConfig(sheet_name="US")},
    {"file": "s06_multi_sheet_same_structure.xlsx", "id": "S06_DE", "config": FileProcessingConfig(sheet_name="DE")},
    {"file": "s06_multi_sheet_same_structure.xlsx", "id": "S06_AU", "config": FileProcessingConfig(sheet_name="AU")},
]

all_regional: list[LoadResult] = []

for cfg in REGIONAL_CONFIGS:
    path      = f"{VOLUME_PATH}/{cfg['file']}"
    structure = framework.detect_structure(path, config=cfg["config"])
    meta      = framework.extract_metadata(path, structure, file_id=cfg["id"])
    result    = framework.load(path, structure, meta)
    all_regional.append(result)
    print(f"  {cfg['id']}: {result.df.count()} rows")

combined_df = framework.combine(all_regional)
print(f"\nCombined: {combined_df.count()} total rows, {len(combined_df.columns)} columns")
display(combined_df)

# COMMAND ----------

# DBTITLE 1,Pattern 4 — Combine Files with Slightly Different Columns (NULL-fill in action)
# S01 (simple) and S07 Extended (wide, 3-level headers) have different columns.
# combine() NULL-fills columns absent from each file.

MIXED_CONFIGS = [
    {"file": "s01_simple_single_sheet.xlsx",      "id": "S01", "config": FileProcessingConfig()},
    {"file": "s07_wide_standard_vs_extended.xlsx", "id": "S07", "config": FileProcessingConfig(sheet_name="Extended", static_header_rows=[1, 2])},
]

mixed_results: list[LoadResult] = []

for cfg in MIXED_CONFIGS:
    path      = f"{VOLUME_PATH}/{cfg['file']}"
    structure = framework.detect_structure(path, config=cfg["config"])
    meta      = framework.extract_metadata(path, structure, file_id=cfg["id"])
    result    = framework.load(path, structure, meta)
    mixed_results.append(result)
    print(f"  {cfg['id']}: {len(result.df.columns)} columns")

combined_mixed = framework.combine(mixed_results)
print(f"\nCombined superset: {len(combined_mixed.columns)} columns (NULL-filled where absent)")
display(combined_mixed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config Overrides — `FileProcessingConfig.from_override()`
# MAGIC
# MAGIC Some files need non-default processing: a specific sheet, explicit header rows,
# MAGIC rows to skip, hidden columns to ignore. Instead of hardcoding these in every script,
# MAGIC store them as data and build the config at runtime.
# MAGIC
# MAGIC `FileProcessingConfig.from_override(row)` accepts a dict or Spark Row and maps
# MAGIC known keys to config fields. Missing or `None` values fall back to auto-detection.

# COMMAND ----------

# DBTITLE 1,Override Example 1 — Manual Dict (hardcoded in notebook)
# Useful during development or for one-off files.
# Keys match FileProcessingConfig field names exactly.

MANUAL_OVERRIDES = {
    "s02_multi_row_merged_headers.xlsx": {
        "sheet_name":         "Product Catalogue",
        "static_header_rows": [1, 2],           # openpyxl can't auto-detect merged header rows
    },
    "s10_blank_column_sections.xlsx": {
        "static_header_rows": [1, 2],
    },
    "s12_wide_complex_3level_headers.xlsx": {
        "sheet_name":         "UK",
        "static_header_rows": [1, 2, 3],
    },
}

# Use override if present; fall back to defaults for files not in the override map
for fname, override_dict in MANUAL_OVERRIDES.items():
    config    = FileProcessingConfig.from_override(override_dict)
    path      = f"{VOLUME_PATH}/{fname}"
    structure = framework.detect_structure(path, config=config)
    meta      = framework.extract_metadata(path, structure)
    result    = framework.load(path, structure, meta)
    print(f"  {fname}: {result.df.count()} rows, {len(result.df.columns)} columns")

# COMMAND ----------

# DBTITLE 1,Override Example 2 — Skip Specific Data Rows at Load Time
# ignore_rows skips rows by 1-based row number during data loading.
# Useful for footer rows, totals rows, or rows with known data quality issues.
# These are load-time skips — they don't affect structure or metadata detection.

path      = f"{VOLUME_PATH}/s01_simple_single_sheet.xlsx"
structure = framework.detect_structure(path)
meta      = framework.extract_metadata(path, structure, file_id="S01")

# Suppose rows 3 and 5 (1-based in the sheet) are known bad rows to skip
override_with_skip = FileProcessingConfig.from_override({
    "ignore_rows": [3, 5],
})

result_full    = framework.load(path, structure, meta)
result_skipped = framework.load(path, structure, meta, config=override_with_skip)

print(f"Full load    : {result_full.df.count()} rows")
print(f"After skip   : {result_skipped.df.count()} rows  (rows 3 & 5 excluded)")

# COMMAND ----------

# DBTITLE 1,Override Example 3 — Delta Table Override Pattern (production-ready)
# Store per-file overrides in a Delta table managed by your team.
# No code changes needed when a file changes its sheet name or adds header rows.
# Schema suggestion for the override table:
#
# | file_name (STRING) | sheet_name (STRING) | static_header_rows (ARRAY<INT>) | data_start_row (INT) | ignore_rows (ARRAY<INT>) | ignore_columns (ARRAY<INT>) |
# |--------------------|---------------------|--------------------------------|----------------------|--------------------------|----------------------------|
# | s02_multi_row...   | Product Catalogue   | [1, 2]                         | null                 | null                     | null                       |
# | s12_wide_compl...  | UK                  | [1, 2, 3]                      | null                 | null                     | null                       |

# --- Uncomment to use ---
#
# overrides_df = spark.table(f"{MY_CATALOG}.{INGEST_SCHEMA}.excel_load_overrides")
# overrides_map = {
#     row["file_name"]: FileProcessingConfig.from_override(row)
#     for row in overrides_df.collect()
# }
#
# for cfg in FILE_CONFIGS:
#     config    = overrides_map.get(cfg["file"], FileProcessingConfig())
#     path      = f"{VOLUME_PATH}/{cfg['file']}"
#     structure = framework.detect_structure(path, config=config)
#     meta      = framework.extract_metadata(path, structure, file_id=cfg["id"])
#     result    = framework.load(path, structure, meta, config=config)
#     result.df.write.mode("append").saveAsTable(cfg["table"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Write to Delta

# COMMAND ----------

# DBTITLE 1,(Optional) Write a Single Result to Delta
# All bronze columns are STRING — add source_file and source_sheet as partition
# columns or tracking columns depending on your table design.

# result.df.write \
#     .mode("append") \
#     .saveAsTable(f"{MY_CATALOG}.{INGEST_SCHEMA}.s01_bronze")

# COMMAND ----------

# DBTITLE 1,(Optional) Write Combined Result to Delta
# framework.combine() returns a plain Spark DataFrame — write it like any other.
# The column set is the union of all files; missing columns land as NULL.

# combined_df.write \
#     .mode("append") \
#     .saveAsTable(f"{MY_CATALOG}.{INGEST_SCHEMA}.s06_all_regions_bronze")

# COMMAND ----------

# DBTITLE 1,(Optional) Generate Bronze Table DDL from Combined Schema
# If you prefer CREATE TABLE first, then INSERT, generate the DDL from result.df.columns.
# All data columns are STRING; auto-columns get their native types.

def generate_bronze_ddl(catalog: str, schema: str, table: str, df) -> str:
    auto = {"source_file", "source_sheet", "insert_timestamp"}
    col_defs = []
    for field in df.schema.fields:
        if field.name not in auto:
            col_defs.append(f"`{field.name}` STRING")
    col_defs += [
        "source_file      STRING",
        "source_sheet     STRING",
        "insert_timestamp TIMESTAMP",
    ]
    return (
        f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (\n"
        + ",\n    ".join("    " + c for c in col_defs)
        + "\n)"
    )

print(generate_bronze_ddl(MY_CATALOG, INGEST_SCHEMA, "s06_all_regions_bronze", combined_df))
# spark.sql(generate_bronze_ddl(...))  # uncomment to execute
