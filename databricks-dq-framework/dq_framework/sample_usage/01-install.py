# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Install & Initialise (run once per session)
# MAGIC Installs the DQ framework, defines catalog/schema variables, instantiates `dq`, calls `dq.setup()`,
# MAGIC and loads sample CSV data as Delta tables.
# MAGIC All other notebooks (`02-*`, `03-run`) start with `%run ./01-install` to inherit these.

# COMMAND ----------

# DBTITLE 1,Install Databricks Data Quality Framework
# MAGIC %pip install databricks-dq-framework --upgrade --no-deps
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Define Catalog and Schema Variables
MY_CATALOG      = "sampledatacatalog"   # ← change to your catalog
CURATED_SCHEMA  = "silver"              # ← schema that holds your curated tables
DQ_SCHEMA       = "dq"                  # ← schema where DQ framework tables will live

# COMMAND ----------

# DBTITLE 1,Check Installed Version
# MAGIC %pip show databricks-dq-framework

# COMMAND ----------

# DBTITLE 1,Import and Initialise DQ Framework
from dq_framework import DQFramework

dq = DQFramework(spark, catalog=MY_CATALOG, schema=DQ_SCHEMA)

# COMMAND ----------

# DBTITLE 1,DQ Framework Setup (creates framework tables if not exist)
dq.setup()

# COMMAND ----------

# DBTITLE 1,Display Current DQ Framework Configuration Summary
dq.config.show_config_summary()

# COMMAND ----------

# DBTITLE 1,Framework Guide
dq.guide()

# COMMAND ----------

# DBTITLE 1,Framework Sample Resources
# Extracts bundled sample files to /Workspace/Users/{you}/databricks-dq-framework/sample_usage/
# Works on all compute types (serverless, classic, no DBFS root required).
# Returns the path for use in subsequent cells.
SAMPLE_USAGE_PATH = dq.sample_usage(spark)

# COMMAND ----------

# DBTITLE 1,Copy Sample CSV Files to Volume
# Copies the 3 sample CSVs from the user workspace path into the Unity Catalog Volume.
# Uses shutil.copy2 (standard Python) — no dbutils.fs, no DBFS dependency.
# Safe to re-run — overwrites any existing files in the volume.
import shutil

VOLUME_PATH = f"/Volumes/{MY_CATALOG}/{CURATED_SCHEMA}/sample_data"
CSV_FILES   = ["mock_curated_vendors", "mock_curated_contacts", "mock_curated_locations"]

for fname in CSV_FILES:
    shutil.copy2(f"{SAMPLE_USAGE_PATH}/{fname}.csv", f"{VOLUME_PATH}/{fname}.csv")
    print(f"✓ Copied {fname}.csv → {VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,Load Sample CSV Files from Volume into Delta Tables
# Reads each CSV from the volume and writes it as a managed Delta table (overwrite — idempotent).
for table_name in CSV_FILES:
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{VOLUME_PATH}/{table_name}.csv"))
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"{MY_CATALOG}.{CURATED_SCHEMA}.{table_name}"))
    print(f"✓ {table_name} — {df.count()} rows loaded")
