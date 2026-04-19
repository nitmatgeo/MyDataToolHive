# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Stage 1: Validate All Sample Files
# MAGIC
# MAGIC Runs `framework.validate()` against every sample file in the Volume and prints
# MAGIC a structured result per file — existence, format, readability, sheets, warnings.

# COMMAND ----------

# DBTITLE 1,Config — point at the Volume created in 00-setup-volume
MY_CATALOG    = "sampledatacatalog"
INGEST_SCHEMA = "bronze"
VOLUME_NAME   = "excel_ingest_samples"
VOLUME_PATH   = f"/Volumes/{MY_CATALOG}/{INGEST_SCHEMA}/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Initialise Framework
from excel_ingest import ExcelIngestFramework, ValidationStatus

framework = ExcelIngestFramework(spark=spark)

# COMMAND ----------

# DBTITLE 1,File List with Passwords
# s11 is AES-encrypted — password required; all others have password=None
FILE_CONFIGS = [
    {"file": "s01_simple_single_sheet.xlsx",         "password": None,           "label": "Simple single sheet"},
    {"file": "s02_multi_row_merged_headers.xlsx",    "password": None,           "label": "Multi-row merged headers"},
    {"file": "s03_no_headers.xlsx",                  "password": None,           "label": "No headers (raw data)"},
    {"file": "s04_headers_only_no_data.xlsx",        "password": None,           "label": "Headers only, no data"},
    {"file": "s05_multi_sheet_diff_structure.xlsx",  "password": None,           "label": "Multi-sheet, different structure"},
    {"file": "s06_multi_sheet_same_structure.xlsx",  "password": None,           "label": "Multi-sheet, same structure"},
    {"file": "s07_wide_standard_vs_extended.xlsx",   "password": None,           "label": "Wide — Standard vs Extended"},
    {"file": "s08_hidden_sheet.xlsx",                "password": None,           "label": "Hidden sheet (_Margins)"},
    {"file": "s09_hidden_columns.xlsx",              "password": None,           "label": "Hidden columns (F, I)"},
    {"file": "s10_blank_column_sections.xlsx",       "password": None,           "label": "Blank column separators"},
    {"file": "s11_password_protected.xlsx",          "password": "Password1234", "label": "Password protected"},
    {"file": "s12_wide_complex_3level_headers.xlsx", "password": None,           "label": "Wide — 3-level merged headers"},
]

# COMMAND ----------

# DBTITLE 1,Validate Each File

PASS  = "PASSED"
WARN  = "WARNING"
FAIL  = "FAILED"
icons = {PASS: "OK", WARN: "WARN", FAIL: "FAIL"}

results = []

for cfg in FILE_CONFIGS:
    path = f"{VOLUME_PATH}/{cfg['file']}"
    r    = framework.validate(path, password=cfg["password"])
    results.append((cfg["label"], r))

    icon = icons.get(r.status.value, "?")
    print(f"[{icon}] {cfg['label']}")
    print(f"       Status   : {r.status.value}")
    print(f"       Format   : {r.format_type}  |  Size: {r.file_size_bytes:,} bytes" if r.file_size_bytes else f"       Format   : {r.format_type}")
    print(f"       Sheets   : {r.all_sheet_names}")
    if r.warnings:
        print(f"       Warnings : {r.warnings}")
    if r.errors:
        print(f"       Errors   : {r.errors}")
    print()

# COMMAND ----------

# DBTITLE 1,Summary

passed  = sum(1 for _, r in results if r.status.value == PASS)
warned  = sum(1 for _, r in results if r.status.value == WARN)
failed  = sum(1 for _, r in results if r.status.value == FAIL)

print(f"Validated {len(results)} files:")
print(f"  PASSED  : {passed}")
print(f"  WARNING : {warned}   (hidden sheets — non-blocking)")
print(f"  FAILED  : {failed}")
