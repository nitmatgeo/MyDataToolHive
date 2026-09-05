# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Install & Initialise (run once per session)
# MAGIC Installs the framework, defines catalog/schema variables, creates the Volume,
# MAGIC and copies the 12 sample Excel files into it.
# MAGIC All other notebooks (02-05) start with `%run ./01-install` to inherit these variables.

# COMMAND ----------

# DBTITLE 1,Install databricks-excel-ingest-framework
%pip install databricks-excel-ingest-framework --upgrade
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Define Catalog and Schema Variables
MY_CATALOG    = "sampledatacatalog"    # <- change to your catalog
INGEST_SCHEMA = "bronze"              # <- ingestion / landing schema
VOLUME_NAME   = "excel_ingest_samples"
VOLUME_PATH   = f"/Volumes/{MY_CATALOG}/{INGEST_SCHEMA}/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Check Installed Version
%pip show databricks-excel-ingest-framework

# COMMAND ----------

# DBTITLE 1,Import and Initialise Framework
from excel_ingest import ExcelIngestFramework

framework = ExcelIngestFramework(spark=spark)

# COMMAND ----------

# DBTITLE 1,Framework Guide
framework.guide()

# COMMAND ----------

# DBTITLE 1,Create Schema and Volume
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{INGEST_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{INGEST_SCHEMA}.{VOLUME_NAME}")
print(f"Volume ready: {VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,Extract Sample Files from Package to Workspace
# Copies bundled notebooks + sample Excel files to /Workspace/Users/{you}/
# Safe to re-run — skips files already up-to-date.
import os, shutil

SAMPLE_USAGE_PATH = framework.sample_usage(spark)
SAMPLES_PATH      = f"{SAMPLE_USAGE_PATH}/samples"

print(f"Sample files at: {SAMPLES_PATH}")

# COMMAND ----------

# DBTITLE 1,Copy Sample Excel Files to Volume
for fname in sorted(os.listdir(SAMPLES_PATH)):
    if not fname.endswith(".xlsx"):
        continue
    shutil.copy2(f"{SAMPLES_PATH}/{fname}", f"{VOLUME_PATH}/{fname}")
    print(f"  copied  {fname}")

print(f"\nDone.")

# COMMAND ----------

# DBTITLE 1,Verify — List Files in Volume
display(spark.createDataFrame([
    {"file": f, "size_bytes": os.path.getsize(f"{VOLUME_PATH}/{f}")}
    for f in sorted(os.listdir(VOLUME_PATH))
]))
