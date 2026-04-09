# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Install & Initialise (run once per session)
# MAGIC Installs the ETL Monitor framework, defines catalog/schema variables, instantiates `monitor`,
# MAGIC calls `monitor.setup()`, and extracts sample notebooks to your workspace.
# MAGIC All other notebooks (`02-config`, `03-run`) start with `%run ./01-install` to inherit these.

# COMMAND ----------

# DBTITLE 1,Install Databricks ETL Monitor Framework
# MAGIC %pip install databricks-etl-monitor --upgrade --no-deps
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Define Catalog and Schema Variables
MY_CATALOG  = "sampledatacatalog"   # ← change to your catalog
ETL_SCHEMA  = "etl"                 # ← schema where ETL monitoring tables will live

# COMMAND ----------

# DBTITLE 1,Check Installed Version
# MAGIC %pip show databricks-etl-monitor

# COMMAND ----------

# DBTITLE 1,Import and Initialise ETL Monitor Framework
from etl_monitor import ETLMonitorFramework

monitor = ETLMonitorFramework(spark, catalog=MY_CATALOG, schema=ETL_SCHEMA)

# COMMAND ----------

# DBTITLE 1,ETL Monitor Setup (creates all 6 tables + 6 views + seeds sequence stages)
# Idempotent — safe to run every cluster start or re-deploy.
# Creates: ETLconfigSequence, ETLconfigProcess, ETLconfigTasks, ETLconfigParameters,
#          ETLProcessingSteps, ETLsysLogs
# Views:   v_processStatus, v_runSummary, v_taskDetail,
#          v_mandatoryBlockers, v_currentFailures, v_watermarks
monitor.setup()

# COMMAND ----------

# DBTITLE 1,Verify Sequence Stages Seeded
spark.sql(f"""
    SELECT SequenceID, SequenceCode, SequenceName, SortOrder
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`ETLconfigSequence`
    ORDER BY SortOrder
""").display()

# COMMAND ----------

# DBTITLE 1,Extract Sample Notebooks to Workspace
# Extracts bundled sample notebooks to /Workspace/Users/{you}/databricks-etl-monitor/sample_usage/
# Works on all compute types (serverless, classic, no DBFS root required).
SAMPLE_USAGE_PATH = monitor.sample_usage(spark)
print(f"Sample notebooks extracted to: {SAMPLE_USAGE_PATH}")
