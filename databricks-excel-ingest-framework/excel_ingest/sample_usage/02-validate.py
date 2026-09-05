# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Stage 1: Validate All Sample Files
# MAGIC
# MAGIC Runs `framework.validate()` against every sample file in the Volume.
# MAGIC One row per file — existence, format, readability, sheet counts, warnings, errors.
# MAGIC
# MAGIC **Key columns:**
# MAGIC
# MAGIC | Column | What it is |
# MAGIC |---|---|
# MAGIC | `status` | PASSED / WARNING / FAILED |
# MAGIC | `status_description` | Plain-English explanation — no docs lookup needed |
# MAGIC | `is_readable` | False means the pipeline cannot proceed |
# MAGIC | `is_password_protected` | True means a password is required at every subsequent stage |
# MAGIC | `visible_sheets` | Sheets the framework will auto-detect or let you pick from |
# MAGIC | `warnings` | Non-blocking issues (e.g. hidden sheets present) |
# MAGIC | `errors` | Blocking issues — resolve before continuing |

# COMMAND ----------

# DBTITLE 1,Install & Inherit Variables
%run ./01-install

# COMMAND ----------

# DBTITLE 1,Config — point at the Volume created in 01-install
MY_CATALOG    = "sampledatacatalog"
INGEST_SCHEMA = "bronze"
VOLUME_NAME   = "excel_ingest_samples"
VOLUME_PATH   = f"/Volumes/{MY_CATALOG}/{INGEST_SCHEMA}/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Initialise Framework
from excel_ingest import ExcelIngestFramework, VALIDATION_RECORD_FIELDS

framework = ExcelIngestFramework(spark=spark)

# COMMAND ----------

# DBTITLE 1,Validation Status Reference
# What each status value means — shown once so the main table is self-explanatory.

from excel_ingest import ValidationStatus

display(spark.createDataFrame([
    {"status": s.value, "description": s.description}
    for s in ValidationStatus
]))

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
# One row per file — sortable and filterable.
# status_description explains the outcome; errors column shows what to fix for FAILED files.

validation_records = []

for cfg in FILE_CONFIGS:
    path = f"{VOLUME_PATH}/{cfg['file']}"
    r    = framework.validate(path, password=cfg["password"])
    validation_records.append(r.summary_record(label=cfg["label"]))

display(spark.createDataFrame(validation_records).select(VALIDATION_RECORD_FIELDS))

# COMMAND ----------

# DBTITLE 1,Summary by Status
# WARNING = hidden sheets present (non-blocking). FAILED = cannot proceed — check errors.

spark.createDataFrame(validation_records).groupBy("status").count().orderBy("status").display()

# COMMAND ----------

# DBTITLE 1,Negative Examples — File Not Found / Invalid Path / Wrong Password
# Demonstrates what FAILED results look like for common error scenarios.

NEGATIVE_CASES = [
    {"path": f"{VOLUME_PATH}/does_not_exist.xlsx",          "label": "File not found — path does not exist"},
    {"path": f"/Volumes/wrong_catalog/bronze/vol/data.xlsx", "label": "Invalid volume path — wrong catalog"},
    {"path": f"{VOLUME_PATH}/report.csv",                   "label": "Wrong file type — .csv not supported"},
    {"path": f"{VOLUME_PATH}/s11_password_protected.xlsx",  "label": "Encrypted — no password supplied",    "password": None},
    {"path": f"{VOLUME_PATH}/s11_password_protected.xlsx",  "label": "Encrypted — wrong password",          "password": "WrongPass"},
    {"path": f"{VOLUME_PATH}/s11_password_protected.xlsx",  "label": "Encrypted — correct password (PASS)", "password": "Password1234"},
]

negative_records = []

for case in NEGATIVE_CASES:
    r = framework.validate(case["path"], password=case.get("password"))
    negative_records.append(r.summary_record(label=case["label"]))

display(spark.createDataFrame(negative_records).select(VALIDATION_RECORD_FIELDS))
