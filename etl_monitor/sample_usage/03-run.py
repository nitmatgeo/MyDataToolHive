# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Run ETL Monitoring & View Results
# MAGIC Demonstrates a full ETL execution run using the ETL Monitor framework:
# MAGIC - Generate execution steps (NQUE rows for all active tasks)
# MAGIC - Simulate task start, completion and failure for HR / EMPLOYEE_MASTER
# MAGIC - Demonstrate mandatory blocker behaviour
# MAGIC - Retry a failed task
# MAGIC - Query results via all 6 reporting views
# MAGIC - Show processing mode switching
# MAGIC
# MAGIC Depends on `01-install` (framework init) and `02-config` (process + task registration).
# MAGIC
# MAGIC **Scenario:** HR / EMPLOYEE_MASTER — 9 tasks across 2 workflows
# MAGIC
# MAGIC | TaskID | WF | SeqID | Task | Mandatory | SourceType |
# MAGIC |---|---|---|---|---|---|
# MAGIC | 0 | 0 | 0 | Initiation | Y | DBX_NOTEBOOK |
# MAGIC | 1 | 1 | 1 | Load HR Reference Config | Y | DBX_NOTEBOOK |
# MAGIC | 2 | 1 | 2 | Load UK Employees (SAP HR) | Y | ADF_PIPELINE |
# MAGIC | 3 | 1 | 2 | Load US Employees (Workday) | Y | DBX_NOTEBOOK |
# MAGIC | 4 | 1 | 2 | Load India Employees (PeopleSoft) | N | DBX_NOTEBOOK |
# MAGIC | 5 | 1 | 2 | Load Org Structure Feed | N | DBX_NOTEBOOK |
# MAGIC | 6 | 2 | 3 | Process Employee Dimensions | Y | DBX_NOTEBOOK |
# MAGIC | 7 | 2 | 5 | Apply HR Business Rules | N | DBX_NOTEBOOK |
# MAGIC | 8 | 2 | 6 | Build Employee Analytics Mart | Y | DBX_NOTEBOOK |
# MAGIC
# MAGIC Tasks 2, 3, 4, 5 share SequenceID=2 — they run **in parallel** in production.
# MAGIC Here they run sequentially for demonstration clarity.

# COMMAND ----------

# DBTITLE 1,Inherit Framework and Variables from 01-install
# MAGIC %run ./01-install

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Generate an Execution ID and Execution Steps

# COMMAND ----------

# DBTITLE 1,Generate a New Execution ID for This Run
# ── ExecutionID sources ────────────────────────────────────────────────────────
# Option A — ADF orchestration: ADF passes pipeline().RunId as a widget value.
#   EXECUTION_ID = dbutils.widgets.get("execution_id")
#
# Option B — Databricks-generated (used here for demo): framework generates a UUID.
#   Use this for direct Databricks Workflow runs or ad-hoc notebook runs.
EXECUTION_ID    = ETLMonitorFramework.generate_execution_id()
PROJECT_CODE    = "HR"
PROCESS_LOAD    = "EMPLOYEE_MASTER"
PROCESSING_DATE = "2026-04-13"   # ← change to your processing date or omit (defaults to today)

print(f"Execution ID     : {EXECUTION_ID}")
print(f"Project / Process: {PROJECT_CODE} / {PROCESS_LOAD}")
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
# processing_date is optional — defaults to today's date when omitted.
monitor.get_pending_tasks(EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Read Current Watermarks

# COMMAND ----------

# DBTITLE 1,Read Watermarks Before Loading
# Each delta task reads its watermark to build the incremental source query.
# NULL = first run / bulk mode (load everything from source).
wm_uk = monitor.get_active_watermark(PROJECT_CODE, PROCESS_LOAD, "LoadEmployeesUK")
wm_us = monitor.get_active_watermark(PROJECT_CODE, PROCESS_LOAD, "LoadEmployeesUS")
wm_in = monitor.get_active_watermark(PROJECT_CODE, PROCESS_LOAD, "LoadEmployeesIN")
wm_org = monitor.get_active_watermark(PROJECT_CODE, PROCESS_LOAD, "LoadOrgStructure")

print(f"UK  employee watermark : {wm_uk}   (None = bulk load)")
print(f"US  employee watermark : {wm_us}   (None = bulk load)")
print(f"IN  employee watermark : {wm_in}   (None = bulk load)")
print(f"Org structure watermark: {wm_org}   (None = bulk load)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Run WorkFlowID=0 (Initiation)

# COMMAND ----------

# DBTITLE 1,Run Initiation Task (WorkFlowID=0, TaskID=0, SequenceID=0)
# The initiation task marks the overall run as started.
# In production this runs in a dedicated notebook — shown inline here for demonstration.
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 0,
    workflow_id     = 0,
    sequence_id     = 0,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "Run initiated successfully",
):
    pass   # initiation task has no logic — just marks the run as started

print("✓ Initiation task completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Run WorkFlowID=1, SequenceID=1 (Config Load)

# COMMAND ----------

# DBTITLE 1,Run Config Load Task (TaskID=1, SequenceID=1)
# Runs first, before the parallel SequenceID=2 ingestion tasks.
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 1,
    workflow_id     = 1,
    sequence_id     = 1,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "Department hierarchy, job grades and cost-centre codes loaded: 312 rows",
):
    import time
    time.sleep(1)   # simulate work

print("✓ Config load task completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Run WorkFlowID=1, SequenceID=2 (Parallel Ingestion)
# MAGIC In production ADF/Databricks Workflow dispatches Tasks 2, 3, 4, 5 in parallel.
# MAGIC They are run sequentially here for demonstration.

# COMMAND ----------

# DBTITLE 1,Simulate UK Employee Load FAILURE (TaskID=2, mandatory ADF_PIPELINE)
# Demonstrates how a mandatory task failure surfaces in v_mandatoryBlockers.
# When a mandatory task FAILs, the initiation task (TaskID=0) is reset to NQUE
# so the overall run is no longer marked as in-progress.
try:
    with monitor.task(
        EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
        task_id         = 2,
        workflow_id     = 1,
        sequence_id     = 2,
        processing_date = PROCESSING_DATE,
        source_type     = "ADF_PIPELINE",
    ):
        raise ConnectionError("SAP HR connection timeout — source system unavailable")
except Exception as e:
    print(f"Task failed (expected for demo): {e}")

print("✓ Failure captured — task status is now FAIL")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Query Status After Mandatory Failure

# COMMAND ----------

# DBTITLE 1,Task Detail — Current State of All Tasks
spark.sql(f"""
    SELECT TaskID, WorkFlowID, SequenceID, SequenceCode, TaskName,
           Status, StartTime, EndTime, DurationSeconds, LogMessage
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_taskDetail`
    WHERE ExecutionID = '{EXECUTION_ID}'
    ORDER BY WorkFlowID, SequenceID, TaskID
""").display()

# COMMAND ----------

# DBTITLE 1,Mandatory Blockers — Tasks Preventing Downstream Progress
# TaskID=2 is mandatory — its FAIL blocks all downstream tasks in this execution.
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

# MAGIC %md
# MAGIC ## Step 7 — Reset Failed Task and Retry

# COMMAND ----------

# DBTITLE 1,Reset UK Employee Task for Retry (FAIL → RQUE)
# Pass task_id + workflow_id to reset a specific task only.
# Omit both to reset all FAIL rows in the run.
monitor.status_reset(
    PROJECT_CODE, PROCESS_LOAD,
    execution_id = EXECUTION_ID,
    task_id      = 2,
    workflow_id  = 1,
)
print("✓ TaskID=2 reset to RQUE — ready for retry")

# COMMAND ----------

# DBTITLE 1,Retry UK Employee Load (ADF_PIPELINE) — Succeeds on Second Attempt
# Attempts counter increments on each retry.
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 2,
    workflow_id     = 1,
    sequence_id     = 2,
    processing_date = PROCESSING_DATE,
    source_type     = "ADF_PIPELINE",
    log_message     = "UK employees loaded from SAP HR: 4,821 records (delta since last watermark)",
):
    import time
    time.sleep(1)   # simulate work

print("✓ UK employee load task completed on retry — DELTA_DATE watermark auto-advanced")

# COMMAND ----------

# DBTITLE 1,US Employee Load (TaskID=3, mandatory)
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 3,
    workflow_id     = 1,
    sequence_id     = 2,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "US employees loaded from Workday ADLS path: 2,340 records",
):
    import time
    time.sleep(1)

print("✓ US employee load completed — DELTA_DATE watermark auto-advanced")

# COMMAND ----------

# DBTITLE 1,India Employee Load (TaskID=4, non-mandatory)
# Non-mandatory — failure here does NOT block downstream stages.
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 4,
    workflow_id     = 1,
    sequence_id     = 2,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "India employees loaded from PeopleSoft network share: 1,107 records",
):
    import time
    time.sleep(1)

print("✓ India employee load completed — DELTA_DATE watermark auto-advanced")

# COMMAND ----------

# DBTITLE 1,Org Structure Feed (TaskID=5, non-mandatory)
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 5,
    workflow_id     = 1,
    sequence_id     = 2,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "Org structure feed loaded from ADLS: 286 reporting nodes",
):
    import time
    time.sleep(1)

print("✓ Org structure feed completed — DELTA_DATE watermark auto-advanced")

# COMMAND ----------

# DBTITLE 1,Verify Watermarks Auto-Advanced After DONE (SequenceID=2 tasks)
# All four DELTA_DATE watermarks were auto-advanced to each task's StartTime on DONE.
spark.sql(f"""
    SELECT ParameterName, ParameterType, ActiveValue, ValueDateTime
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_watermarks`
    WHERE ProjectCode = 'HR'
      AND ProcessLoad = 'EMPLOYEE_MASTER'
    ORDER BY ParameterName
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Run WorkFlowID=2 (Processing)

# COMMAND ----------

# DBTITLE 1,Process Employee Dimensions (TaskID=6, SequenceID=3, mandatory)
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 6,
    workflow_id     = 2,
    sequence_id     = 3,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "Employee dimension table merged: 8,268 active employees after deduplication",
):
    import time
    time.sleep(1)

print("✓ Employee dimension processing completed")

# COMMAND ----------

# DBTITLE 1,Apply HR Business Rules (TaskID=7, SequenceID=5, non-mandatory)
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 7,
    workflow_id     = 2,
    sequence_id     = 5,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "FTE classification, band mapping and headcount flags applied: 8,268 rows",
):
    import time
    time.sleep(1)

print("✓ HR business rules applied")

# COMMAND ----------

# DBTITLE 1,Build Employee Analytics Mart (TaskID=8, SequenceID=6, mandatory)
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 8,
    workflow_id     = 2,
    sequence_id     = 6,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "Analytics mart refreshed: headcount 8,268 | attrition rate 4.2% | 3 regions",
):
    import time
    time.sleep(1)

print("✓ Employee analytics mart built — run complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Final Status Queries

# COMMAND ----------

# DBTITLE 1,Task Detail — All Tasks DONE
monitor.get_status(PROJECT_CODE, PROCESS_LOAD, execution_id=EXECUTION_ID).display()

# COMMAND ----------

# DBTITLE 1,Run Summary — This Execution
monitor.get_status(PROJECT_CODE, PROCESS_LOAD, summary_mode=True).display()

# COMMAND ----------

# DBTITLE 1,Process Status — Cross-Process Dashboard
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_processStatus`
    WHERE ProcessingDate = '{PROCESSING_DATE}'
    ORDER BY ProjectCode, ProcessLoad
""").display()

# COMMAND ----------

# DBTITLE 1,Run Summary View — All Executions Today
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_runSummary`
    WHERE ProcessingDate = '{PROCESSING_DATE}'
    ORDER BY ProjectCode, ProcessLoad
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Processing Mode Examples

# COMMAND ----------

# DBTITLE 1,Switch to Bulk (Full Reload) Mode
# Sets all DELTA_DATE watermarks to NULL → next run performs a full reload from source.
monitor.set_processing_mode(PROJECT_CODE, PROCESS_LOAD, is_bulk_mode=True)
print("✓ Bulk mode enabled — next run will perform a full reload for all delta tasks")

# COMMAND ----------

# DBTITLE 1,Switch to Historic Rerun Mode
# Sets SYSDT to a specific past date — useful for replaying a historical processing date.
monitor.set_processing_mode(
    PROJECT_CODE, PROCESS_LOAD,
    is_historic_mode = True,
    historic_date    = "2026-03-01",
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
    "ETLOrganisation",
    "ETLconfigProject",
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
