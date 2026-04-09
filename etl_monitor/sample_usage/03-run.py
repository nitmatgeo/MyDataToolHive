# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Run ETL Monitoring & View Results
# MAGIC Demonstrates a full ETL execution run using the ETL Monitor framework:
# MAGIC - Generate execution steps (NQUE rows for all active tasks)
# MAGIC - Simulate task start, completion and failure
# MAGIC - Retry a failed task
# MAGIC - Query results via all 6 reporting views
# MAGIC
# MAGIC Depends on `01-install` (framework init) and `02-config` (process + task registration).

# COMMAND ----------

# DBTITLE 1,Inherit Framework and Variables from 01-install
# MAGIC %run ./01-install

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Generate an Execution ID and Execution Steps

# COMMAND ----------

# DBTITLE 1,Generate a New Execution ID for This Run
# ExecutionID is a UUID that groups all task rows belonging to a single logical run.
# In production this is passed in as a widget from ADF or Databricks Workflow.
EXECUTION_ID    = ETLMonitorFramework.generate_execution_id()
PROJECT_CODE    = "RETAIL_PLATFORM"
PROCESS_LOAD    = "DAILY_LOAD"
PROCESSING_DATE = "2026-04-09"   # ← change to your processing date or use current_date()

print(f"Execution ID     : {EXECUTION_ID}")
print(f"Processing date  : {PROCESSING_DATE}")

# COMMAND ----------

# DBTITLE 1,Generate Execution Steps (NQUE rows for all active tasks)
# Creates one NQUE row per active task in ETLProcessingSteps.
# Snapshot columns (TaskName, SequenceCode, TaskMandatory, SourceSystemCode)
# are captured at this point — history remains accurate even if the catalogue changes later.
monitor.generate_execution_steps(EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD, PROCESSING_DATE)
print(f"✓ Execution steps generated for ExecutionID: {EXECUTION_ID}")

# COMMAND ----------

# DBTITLE 1,View All Pending Tasks for This Execution
monitor.get_pending_tasks(PROJECT_CODE, PROCESS_LOAD, EXECUTION_ID).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Simulate Task Execution

# COMMAND ----------

# DBTITLE 1,Read Current Watermark Before Loading
# Your notebook reads the delta watermark to build its source query.
products_watermark    = monitor.get_active_watermark(PROJECT_CODE, PROCESS_LOAD, "LoadProducts")
transaction_watermark = monitor.get_active_watermark(PROJECT_CODE, PROCESS_LOAD, "LoadTransactions")

print(f"Products watermark    : {products_watermark}   (None = bulk load)")
print(f"Transactions watermark: {transaction_watermark} (None = bulk load)")

# COMMAND ----------

# DBTITLE 1,Run Initiation Task (WorkFlowID=0, TaskID=0)
# The initiation task marks the overall run as started.
# In production this runs in a dedicated notebook — shown inline here for demonstration.
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id        = 0,
    workflow_id    = 0,
    sequence_id    = 0,
    processing_date= PROCESSING_DATE,
    source_type    = "DBX_NOTEBOOK",
    log_message    = "Run initiated successfully",
):
    pass   # initiation task has no logic — just marks the run as started

print("✓ Initiation task completed")

# COMMAND ----------

# DBTITLE 1,Run Config Load Task (WorkFlowID=1, TaskID=1, SequenceID=1)
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id        = 1,
    workflow_id    = 1,
    sequence_id    = 1,
    processing_date= PROCESSING_DATE,
    source_type    = "DBX_NOTEBOOK",
    log_message    = "Store and product category reference data loaded: 847 rows",
):
    # --- replace with your notebook logic ---
    import time
    time.sleep(1)   # simulate work

print("✓ Config load task completed")

# COMMAND ----------

# DBTITLE 1,Run Products Load Task (WorkFlowID=1, TaskID=2, SequenceID=2)
# In a real workflow, Task 2 and Task 3 run in parallel (same SequenceID=2).
# The ADF ForEach or Databricks Workflow fan-out handles the parallelism.
# Here we run them sequentially for demonstration.
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id        = 2,
    workflow_id    = 1,
    sequence_id    = 2,
    processing_date= PROCESSING_DATE,
    source_type    = "DBX_NOTEBOOK",
    log_message    = "Product catalogue delta loaded: 1,243 rows (modified since last run)",
):
    import time
    time.sleep(1)   # simulate work

print("✓ Products load task completed — DELTA_DATE watermark auto-advanced")

# COMMAND ----------

# DBTITLE 1,Simulate a Transaction Load FAILURE (WorkFlowID=1, TaskID=3)
# Demonstrates how a mandatory task failure is captured and surfaces in v_mandatoryBlockers.
# The initiation task (TaskID=0) is also reset to NQUE when a mandatory task fails.
try:
    with monitor.task(
        EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
        task_id        = 3,
        workflow_id    = 1,
        sequence_id    = 2,
        processing_date= PROCESSING_DATE,
        source_type    = "DBX_NOTEBOOK",
    ):
        raise ValueError("Source POS system connection timeout — retry in next run")
except Exception as e:
    print(f"Task failed (expected for demo): {e}")

print("✓ Failure captured — task status is now FAIL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Query Status After Failure

# COMMAND ----------

# DBTITLE 1,Task Detail View — All Tasks in This Execution
spark.sql(f"""
    SELECT TaskID, WorkFlowID, SequenceID, SequenceCode, TaskName,
           Status, StartTime, EndTime, DurationSeconds, LogMessage
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_taskDetail`
    WHERE ExecutionID = '{EXECUTION_ID}'
    ORDER BY WorkFlowID, SequenceID, TaskID
""").display()

# COMMAND ----------

# DBTITLE 1,Mandatory Blockers — Tasks Preventing Downstream Progress
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_mandatoryBlockers`
    WHERE ExecutionID = '{EXECUTION_ID}'
""").display()

# COMMAND ----------

# DBTITLE 1,Current Failures — All Failed Tasks Today
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_currentFailures`
    WHERE ProcessingDate = '{PROCESSING_DATE}'
    ORDER BY ProjectCode, ProcessLoad, WorkFlowID, TaskID
""").display()

# COMMAND ----------

# DBTITLE 1,Process Status Summary — Cross-Process Dashboard
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_processStatus`
    WHERE ProcessingDate = '{PROCESSING_DATE}'
    ORDER BY ProjectCode, ProcessLoad
""").display()

# COMMAND ----------

# DBTITLE 1,Run Summary — All Executions Today
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_runSummary`
    WHERE ProcessingDate = '{PROCESSING_DATE}'
    ORDER BY ProjectCode, ProcessLoad
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Reset Failed Task and Retry

# COMMAND ----------

# DBTITLE 1,Reset Failed Transaction Task for Retry (FAIL → RQUE)
# status_reset() resets all FAIL rows in the run to RQUE.
# To reset a specific task only, pass task_id and workflow_id.
monitor.status_reset(
    PROJECT_CODE, PROCESS_LOAD,
    execution_id = EXECUTION_ID,
    task_id      = 3,
    workflow_id  = 1,
)
print("✓ Task 3 reset to RQUE — ready for retry")

# COMMAND ----------

# DBTITLE 1,Retry Transaction Load Task
# Attempt counter (Attempts column) increments on each retry.
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id        = 3,
    workflow_id    = 1,
    sequence_id    = 2,
    processing_date= PROCESSING_DATE,
    source_type    = "DBX_NOTEBOOK",
    log_message    = "Transaction load succeeded on retry: 28,456 rows",
):
    import time
    time.sleep(1)   # simulate work

print("✓ Transaction load task completed on retry")

# COMMAND ----------

# DBTITLE 1,Verify Watermarks Auto-Advanced After DONE
# DELTA_DATE watermarks for LoadProducts and LoadTransactions were auto-advanced
# to each task's StartTime when the task reached DONE status.
spark.sql(f"""
    SELECT ParameterName, ParameterType, ActiveValue, ValueDateTime
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_watermarks`
    WHERE ProjectCode = 'RETAIL_PLATFORM'
      AND ProcessLoad = 'DAILY_LOAD'
    ORDER BY ParameterName
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Complete the Run

# COMMAND ----------

# DBTITLE 1,Run Processing Task (WorkFlowID=2, TaskID=4)
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id        = 4,
    workflow_id    = 2,
    sequence_id    = 6,
    processing_date= PROCESSING_DATE,
    source_type    = "DBX_NOTEBOOK",
    log_message    = "Daily sales summary aggregated: 156 store-product combinations",
):
    import time
    time.sleep(1)   # simulate work

print("✓ Processing task completed")

# COMMAND ----------

# DBTITLE 1,Final Status — All Tasks DONE
monitor.get_status(PROJECT_CODE, PROCESS_LOAD, execution_id=EXECUTION_ID).display()

# COMMAND ----------

# DBTITLE 1,Final Run Summary
monitor.get_status(PROJECT_CODE, PROCESS_LOAD, summary_mode=True).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Mode Examples

# COMMAND ----------

# DBTITLE 1,Switch to Bulk (Full Reload) Mode
# Sets SYSDT and all DELTA_DATE watermarks to NULL → next run does a full load.
monitor.set_processing_mode(PROJECT_CODE, PROCESS_LOAD, is_bulk_mode=True)
print("✓ Bulk mode enabled — next run will perform a full reload")

# COMMAND ----------

# DBTITLE 1,Switch to Historic Rerun Mode
# Sets SYSDT to a specific past date — useful for replaying a historical processing date.
monitor.set_processing_mode(
    PROJECT_CODE, PROCESS_LOAD,
    is_historic_mode  = True,
    historic_date     = "2026-03-01",
)
print("✓ Historic mode enabled — SYSDT = 2026-03-01")

# COMMAND ----------

# DBTITLE 1,Restore Live Mode (default)
# Clears SYSDT and restores normal incremental operation.
monitor.set_processing_mode(PROJECT_CODE, PROCESS_LOAD)
print("✓ Live mode restored")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC Run the cell below **only** if you want to tear down all ETL monitoring tables and views from this demo.

# COMMAND ----------

# DBTITLE 1,Teardown — Drop All ETL Monitoring Tables and Views
list_tables = [
    "ETLconfigSequence",
    "ETLconfigProcess",
    "ETLconfigTasks",
    "ETLconfigParameters",
    "ETLProcessingSteps",
    "ETLsysLogs",
]
list_views = [
    "v_processStatus",
    "v_runSummary",
    "v_taskDetail",
    "v_mandatoryBlockers",
    "v_currentFailures",
    "v_watermarks",
]

for table in list_tables:
    spark.sql(f"DROP TABLE IF EXISTS `{MY_CATALOG}`.`{ETL_SCHEMA}`.`{table}`")
    print(f"  dropped table : {table}")
for view in list_views:
    spark.sql(f"DROP VIEW IF EXISTS `{MY_CATALOG}`.`{ETL_SCHEMA}`.`{view}`")
    print(f"  dropped view  : {view}")

spark.sql(f"DROP SCHEMA IF EXISTS `{MY_CATALOG}`.`{ETL_SCHEMA}` CASCADE")
print(f"  dropped schema: {MY_CATALOG}.{ETL_SCHEMA}")

print("✓ Teardown complete")
