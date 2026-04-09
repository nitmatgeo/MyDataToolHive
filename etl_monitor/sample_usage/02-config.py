# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Register Process, Tasks and Parameters
# MAGIC Demonstrates how to register a domain process, its tasks, and delta watermark parameters
# MAGIC using the ETL Monitor framework Python API.
# MAGIC
# MAGIC **Scenario:** `RETAIL_PLATFORM / DAILY_LOAD`
# MAGIC - 1 initiation task (WorkFlowID=0)
# MAGIC - 3 data load tasks in WorkFlowID=1 (config load, products, transactions)
# MAGIC - 1 processing task in WorkFlowID=2 (daily sales summary)
# MAGIC
# MAGIC This notebook is idempotent — all writes use INSERT-ONLY MERGE, so re-running is safe.

# COMMAND ----------

# DBTITLE 1,Inherit Framework and Variables from 01-install
# MAGIC %run ./01-install

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Register the Domain Process

# COMMAND ----------

# DBTITLE 1,Register RETAIL_PLATFORM / DAILY_LOAD Process
# One process row per domain load.  Idempotent — INSERT-ONLY MERGE, never overwrites.
monitor.register_process(
    project_code  = "RETAIL_PLATFORM",
    process_load  = "DAILY_LOAD",
    name          = "Retail Platform Daily Load",
    description   = "Daily ingestion of product catalogue, transaction data, and sales aggregation",
    owner         = "Retail Data Engineering Team",
    load_frequency= "D",
)

print("✓ Process registered: RETAIL_PLATFORM / DAILY_LOAD")

# COMMAND ----------

# DBTITLE 1,Verify Process Registration
spark.sql(f"""
    SELECT ProjectCode, ProcessLoad, ProcessName, LoadFrequency, IsActive
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`ETLconfigProcess`
    WHERE ProjectCode = 'RETAIL_PLATFORM'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Register Tasks
# MAGIC
# MAGIC | TaskID | WorkFlowID | SequenceID | Task | Notes |
# MAGIC |--------|------------|------------|------|-------|
# MAGIC | 0 | 0 | 0 | Initiation | Overall run marker — always WorkFlowID=0 |
# MAGIC | 1 | 1 | 1 | Load Config | Ingest reference/config data from source |
# MAGIC | 2 | 1 | 2 | Load Products | Ingest product catalogue (delta watermark) |
# MAGIC | 3 | 1 | 2 | Load Transactions | Ingest sales transactions in parallel with products |
# MAGIC | 4 | 2 | 6 | Process Daily Summary | Aggregate into sales summary mart |
# MAGIC
# MAGIC Tasks 2 and 3 share SequenceID=2 → they run **in parallel** in the same workflow pass.

# COMMAND ----------

# DBTITLE 1,Register Initiation Task (WorkFlowID=0, always TaskID=0)
monitor.register_task(
    project_code   = "RETAIL_PLATFORM",
    process_load   = "DAILY_LOAD",
    task_id        = 0,
    workflow_id    = 0,
    sequence_id    = 0,
    task_name      = "Initiation",
    task_description = "Overall run initiation marker — resets to NQUE on any mandatory FAIL",
    source_type    = "DBX_NOTEBOOK",
    source_identifier = "/Repos/retail-platform/pipelines/00-initiation",
    task_mandatory = True,
)

# COMMAND ----------

# DBTITLE 1,Register WorkFlowID=1 Tasks (Data Ingestion Pass)
# Task 1 — Load config / reference data (SequenceID=1, runs first)
monitor.register_task(
    project_code   = "RETAIL_PLATFORM",
    process_load   = "DAILY_LOAD",
    task_id        = 1,
    workflow_id    = 1,
    sequence_id    = 1,     # LOAD_DB_CONFIG — runs before tasks at SequenceID=2
    task_name      = "Load Config Data",
    task_description = "Ingest store lookup and product category reference data from source",
    source_type    = "DBX_NOTEBOOK",
    source_identifier = "/Repos/retail-platform/pipelines/01-load-config",
    task_mandatory = True,
    expected_duration_seconds = 120,
)

# Task 2 — Load Products (SequenceID=2, parallel with Task 3)
monitor.register_task(
    project_code   = "RETAIL_PLATFORM",
    process_load   = "DAILY_LOAD",
    task_id        = 2,
    workflow_id    = 1,
    sequence_id    = 2,     # LOAD_DB_TRAN — parallel with Task 3
    task_name      = "Load Products",
    task_description = "Delta ingest of product catalogue changes from source ERP",
    source_type    = "DBX_NOTEBOOK",
    source_identifier = "/Repos/retail-platform/pipelines/02-load-products",
    source_system_code= "LoadProducts",   # links to ETLconfigParameters.ParameterName
    task_mandatory = True,
    expected_duration_seconds = 300,
)

# Task 3 — Load Transactions (SequenceID=2, parallel with Task 2)
monitor.register_task(
    project_code   = "RETAIL_PLATFORM",
    process_load   = "DAILY_LOAD",
    task_id        = 3,
    workflow_id    = 1,
    sequence_id    = 2,     # LOAD_DB_TRAN — parallel with Task 2
    task_name      = "Load Transactions",
    task_description = "Delta ingest of daily sales transactions from source POS system",
    source_type    = "DBX_NOTEBOOK",
    source_identifier = "/Repos/retail-platform/pipelines/03-load-transactions",
    source_system_code= "LoadTransactions",
    task_mandatory = True,
    expected_duration_seconds = 600,
)

print("✓ WorkFlowID=1 tasks registered")

# COMMAND ----------

# DBTITLE 1,Register WorkFlowID=2 Task (Processing Pass)
# Task 4 — Process Daily Summary (SequenceID=6, after all ingestion is complete)
monitor.register_task(
    project_code   = "RETAIL_PLATFORM",
    process_load   = "DAILY_LOAD",
    task_id        = 4,
    workflow_id    = 2,
    sequence_id    = 6,     # PROCESS_DATA — core transformation
    task_name      = "Process Daily Sales Summary",
    task_description = "Aggregate product and transaction data into daily sales summary mart",
    source_type    = "DBX_NOTEBOOK",
    source_identifier = "/Repos/retail-platform/pipelines/04-process-summary",
    task_mandatory = True,
    expected_duration_seconds = 900,
)

print("✓ WorkFlowID=2 task registered")

# COMMAND ----------

# DBTITLE 1,Verify Task Registration
spark.sql(f"""
    SELECT t.TaskID, t.WorkFlowID, t.SequenceID, s.SequenceCode,
           t.TaskName, t.SourceSystemCode, t.TaskMandatory, t.ExpectedDurationSeconds
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`ETLconfigTasks` t
    JOIN `{MY_CATALOG}`.`{ETL_SCHEMA}`.`ETLconfigSequence` s
      ON t.SequenceID = s.SequenceID
    WHERE t.ProjectCode = 'RETAIL_PLATFORM'
      AND t.ProcessLoad = 'DAILY_LOAD'
    ORDER BY t.WorkFlowID, t.SequenceID, t.TaskID
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Register Delta Watermark Parameters
# MAGIC
# MAGIC | ParameterName | ParameterType | Purpose |
# MAGIC |---------------|---------------|---------|
# MAGIC | SYSDT | SYSTEM | Processing system date — NULL = live date |
# MAGIC | LoadProducts | DELTA_DATE | Auto-advanced to task StartTime on DONE |
# MAGIC | LoadTransactions | DELTA_DATE | Auto-advanced to task StartTime on DONE |

# COMMAND ----------

# DBTITLE 1,Register SYSDT (System Processing Date)
# SYSTEM type — controls bulk / historic / live mode via set_processing_mode()
# NULL ValueDateTime = live mode (uses current_date())
monitor.register_parameter(
    project_code    = "RETAIL_PLATFORM",
    process_load    = "DAILY_LOAD",
    parameter_name  = "SYSDT",
    parameter_type  = "SYSTEM",
    description     = "System processing date. NULL = live date. Set to a past date for historic reruns.",
)

# COMMAND ----------

# DBTITLE 1,Register DELTA_DATE Watermarks for Incremental Loads
# LoadProducts — auto-advanced by framework to task StartTime when task reaches DONE
monitor.register_parameter(
    project_code    = "RETAIL_PLATFORM",
    process_load    = "DAILY_LOAD",
    parameter_name  = "LoadProducts",
    parameter_type  = "DELTA_DATE",
    description     = "Last successful product load timestamp. NULL = bulk (full) load.",
)

# LoadTransactions — auto-advanced by framework to task StartTime when task reaches DONE
monitor.register_parameter(
    project_code    = "RETAIL_PLATFORM",
    process_load    = "DAILY_LOAD",
    parameter_name  = "LoadTransactions",
    parameter_type  = "DELTA_DATE",
    description     = "Last successful transaction load timestamp. NULL = bulk (full) load.",
)

print("✓ Parameters registered")

# COMMAND ----------

# DBTITLE 1,Verify Parameter Registration and Current Watermark Values
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_watermarks`
    WHERE ProjectCode = 'RETAIL_PLATFORM'
      AND ProcessLoad = 'DAILY_LOAD'
    ORDER BY ParameterName
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Complete
# MAGIC
# MAGIC The process, tasks, and watermark parameters are now registered.
# MAGIC
# MAGIC **Next step:** run `03-run.py` to simulate a full ETL execution run,
# MAGIC including task start/end/fail events and status queries via all 6 reporting views.
# MAGIC
# MAGIC **Note on DELTA_DATE watermarks:**
# MAGIC The framework auto-advances `DELTA_DATE` watermarks to the task's `StartTime`
# MAGIC when the task reaches `DONE` status.  Your notebook reads the current watermark
# MAGIC via `monitor.get_active_watermark()` or directly from `v_watermarks.ActiveValue`
# MAGIC (for ADF Lookup activities).
# MAGIC
# MAGIC **Note on DELTA_ID watermarks (not used here):**
# MAGIC If your source uses an integer surrogate key, register as `DELTA_ID` and call
# MAGIC `monitor.advance_watermark()` manually after your load — the framework cannot
# MAGIC auto-detect the max ID from the source dataset.
