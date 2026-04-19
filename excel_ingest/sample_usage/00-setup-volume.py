# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Create Volume and Upload Sample Files
# MAGIC
# MAGIC Creates a Unity Catalog Volume under `sampledatacatalog.bronze` and copies the 12
# MAGIC sample Excel files from the repo into it. Run this once before notebooks 02–05.

# COMMAND ----------

# DBTITLE 1,Define Catalog and Volume Variables
MY_CATALOG   = "sampledatacatalog"    # <- change to your catalog
INGEST_SCHEMA = "bronze"              # <- ingestion / landing schema
VOLUME_NAME  = "excel_ingest_samples"
VOLUME_PATH  = f"/Volumes/{MY_CATALOG}/{INGEST_SCHEMA}/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Create Schema and Volume
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{INGEST_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{INGEST_SCHEMA}.{VOLUME_NAME}")
print(f"Volume ready: {VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,Locate Sample Files (via framework.sample_usage)
from excel_ingest import ExcelIngestFramework

framework = ExcelIngestFramework(spark=spark)

# Extracts bundled notebooks to /Workspace/Users/{you}/databricks-excel-ingest-framework/sample_usage/
# Returns the path — we need the samples/ subfolder
SAMPLE_USAGE_PATH = framework.sample_usage(spark)
SAMPLES_PATH      = f"{SAMPLE_USAGE_PATH}/samples"

print(f"Sample files path: {SAMPLES_PATH}")

# COMMAND ----------

# DBTITLE 1,Copy Sample Excel Files to Volume
import os, shutil

SAMPLE_FILES = [
    "s01_simple_single_sheet.xlsx",
    "s02_multi_row_merged_headers.xlsx",
    "s03_no_headers.xlsx",
    "s04_headers_only_no_data.xlsx",
    "s05_multi_sheet_diff_structure.xlsx",
    "s06_multi_sheet_same_structure.xlsx",
    "s07_wide_standard_vs_extended.xlsx",
    "s08_hidden_sheet.xlsx",
    "s09_hidden_columns.xlsx",
    "s10_blank_column_sections.xlsx",
    "s11_password_protected.xlsx",
    "s12_wide_complex_3level_headers.xlsx",
]

for fname in SAMPLE_FILES:
    src  = f"{SAMPLES_PATH}/{fname}"
    dest = f"{VOLUME_PATH}/{fname}"
    shutil.copy2(src, dest)
    print(f"  copied  {fname}")

print(f"\nDone. {len(SAMPLE_FILES)} file(s) in {VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,Verify — List Files in Volume
print(f"Files in {VOLUME_PATH}:\n")
for f in sorted(os.listdir(VOLUME_PATH)):
    size = os.path.getsize(f"{VOLUME_PATH}/{f}")
    print(f"  {f:<58}  {size:>8,} bytes")
