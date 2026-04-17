# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Run ETL Monitoring & View Results
# MAGIC Demonstrates a full ETL execution run using the ETL Monitor framework:
# MAGIC - Generate execution steps (NQUE rows for all active tasks)
# MAGIC - Simulate task start, completion and failure for HR / EMPLOYEE_MASTER
# MAGIC - Demonstrate mandatory blocker behaviour
# MAGIC - Retry via a new ExecutionID — framework skips DONE tasks, picks up FAIL/NQUE only
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
# MAGIC
# MAGIC ## Retry pattern — how it works
# MAGIC When a run fails, the next attempt uses a **new ExecutionID** (mirrors how ADF issues
# MAGIC a new pipeline RunID on re-trigger). Calling `generate_execution_steps` with the new ID:
# MAGIC - Detects existing rows on this date → Attempts = MAX + 1
# MAGIC - Skips tasks already DONE (carries forward without re-running)
# MAGIC - Creates new NQUE rows (Attempts=1) only for FAIL / NQUE tasks
# MAGIC
# MAGIC `status_reset()` is for **day replay** (re-running an already-completed day), not failure retry.

# COMMAND ----------

# DBTITLE 1,Inherit Framework and Variables from 01-install
# MAGIC %run ./01-install

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Generate Execution ID and Steps (Run 1, Attempts=0)

# COMMAND ----------

# DBTITLE 1,Generate Execution ID for Run 1
# ── ExecutionID sources ────────────────────────────────────────────────────────
# Option A — ADF orchestration: ADF passes pipeline().RunId as a widget value.
#   EXECUTION_ID = dbutils.widgets.get("execution_id")
#
# Option B — Databricks-generated (used here for demo): framework generates a UUID.
EXECUTION_ID    = ETLMonitorFramework.generate_execution_id()
PROJECT_CODE    = "HR"
PROCESS_LOAD    = "EMPLOYEE_MASTER"
PROCESSING_DATE = "2026-04-15"

print(f"Run 1 Execution ID : {EXECUTION_ID}")
print(f"Project / Process  : {PROJECT_CODE} / {PROCESS_LOAD}")
print(f"Processing date    : {PROCESSING_DATE}")

# COMMAND ----------

# DBTITLE 1,Generate Execution Steps — Run 1 (Attempts=0, all 9 tasks)
# First call for this date → Attempts=0, all active tasks inserted as NQUE.
monitor.generate_execution_steps(EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD, PROCESSING_DATE)
print(f"✓ Execution steps generated — Attempts=0, 9 tasks queued as NQUE")

# COMMAND ----------

# DBTITLE 1,View All Pending Tasks — Run 1
# get_pending_tasks() output includes:
#   Status, WorkFlowID, SequenceID, TaskID, TaskName, SequenceCode
#   FullFileName  — computed file name (e.g. payroll_uk_202604.csv); NULL for non-file tasks
#   InFilePath    — ADLS base folder path; ADF combines with FullFileName for file URL
#   WatermarkType — DELTA_DATE / DELTA_ID / FLAG / NULL (no watermark)
#   WatermarkValue — resolved watermark value as STRING; ADF uses in @concat() source queries
#     Equivalent to the #DELTAPARAMETER# value from the original SQL Server framework
monitor.get_pending_tasks(EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD, PROCESSING_DATE).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Read Current Watermarks

# COMMAND ----------

# DBTITLE 1,Read Watermarks Before Loading
wm_uk  = monitor.get_active_watermark(PROJECT_CODE, PROCESS_LOAD, "LoadEmployeesUK")
wm_us  = monitor.get_active_watermark(PROJECT_CODE, PROCESS_LOAD, "LoadEmployeesUS")
wm_in  = monitor.get_active_watermark(PROJECT_CODE, PROCESS_LOAD, "LoadEmployeesIN")
wm_org = monitor.get_active_watermark(PROJECT_CODE, PROCESS_LOAD, "LoadOrgStructure")

print(f"UK  employee watermark : {wm_uk}   (None = bulk load)")
print(f"US  employee watermark : {wm_us}   (None = bulk load)")
print(f"IN  employee watermark : {wm_in}   (None = bulk load)")
print(f"Org structure watermark: {wm_org}   (None = bulk load)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Run WorkFlowID=0 (Initiation)

# COMMAND ----------

# DBTITLE 1,Run Initiation Task (TaskID=0, WF=0, SeqID=0)
with monitor.task(
    EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 0,
    workflow_id     = 0,
    sequence_id     = 0,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "Run initiated successfully",
):
    pass

print("✓ Initiation task completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Run WorkFlowID=1, SequenceID=1 (Config Load)

# COMMAND ----------

# DBTITLE 1,Run Config Load Task (TaskID=1, SeqID=1)
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
    time.sleep(1)

print("✓ Config load task completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Run WorkFlowID=1, SequenceID=2 (Parallel Ingestion)
# MAGIC In production ADF/Databricks Workflow dispatches Tasks 2, 3, 4, 5 in parallel.
# MAGIC Run sequentially here for demonstration.

# COMMAND ----------

# DBTITLE 1,Simulate UK Employee Load FAILURE (TaskID=2, mandatory ADF_PIPELINE)
# A mandatory task FAIL resets the initiation task (WF=0, Seq=0) to NQUE automatically,
# marking the overall run as no longer in-progress.
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

print("✓ Failure captured — TaskID=2 is now FAIL, LOAD_GO reset to NQUE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Query Status After Failure

# COMMAND ----------

# DBTITLE 1,Task Detail — State After Failure (Run 1)
spark.sql(f"""
    SELECT WorkFlowID, SequenceID, TaskID, SequenceCode, TaskName,
           Attempts, Status, StartTime, EndTime, DurationSeconds, LogMessage
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_taskDetail`
    WHERE ExecutionID = '{EXECUTION_ID}'
    ORDER BY WorkFlowID, SequenceID, TaskID
""").display()

# COMMAND ----------

# DBTITLE 1,Mandatory Blockers — TaskID=2 Blocking All Downstream Tasks
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_mandatoryBlockers`
    WHERE ExecutionID = '{EXECUTION_ID}'
""").display()

# COMMAND ----------

# DBTITLE 1,Current Failures — All Failed Tasks Today
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_currentFailures`
    WHERE ProcessingDate = '{PROCESSING_DATE}'
    ORDER BY ProjectCode, ProcessLoad, WorkFlowID, SequenceID, TaskID
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Retry via New ExecutionID (Attempts=1)
# MAGIC
# MAGIC In production, ADF issues a new pipeline RunID when it re-triggers after failure.
# MAGIC That new RunID becomes the new ExecutionID. Here we simulate the same pattern.
# MAGIC
# MAGIC `generate_execution_steps` with the new ID:
# MAGIC - Detects existing rows on `2026-04-13` → Attempts = 0+1 = **1**
# MAGIC - TaskID=1 (config): already DONE at Attempts=0 → **skipped, not re-run**
# MAGIC - TaskID=2 (UK load): FAIL at Attempts=0, no DONE row → **new NQUE row at Attempts=1**
# MAGIC - TaskID=3,4,5: NQUE at Attempts=0 (not yet started) → **new NQUE rows at Attempts=1**
# MAGIC - TaskID=0,6,7,8: NQUE/not-DONE → **new NQUE rows at Attempts=1**

# COMMAND ----------

# DBTITLE 1,Simulate ADF Re-trigger — New ExecutionID
# In ADF: this is automatically pipeline().RunId of the re-triggered pipeline.
# Here: generate a fresh UUID to simulate the re-trigger.
EXECUTION_ID_2 = ETLMonitorFramework.generate_execution_id()
print(f"Run 2 Execution ID : {EXECUTION_ID_2}   (Attempts=1 — retry)")

# COMMAND ----------

# DBTITLE 1,Generate Execution Steps — Run 2 (Attempts=1, skips DONE tasks)
monitor.generate_execution_steps(EXECUTION_ID_2, PROJECT_CODE, PROCESS_LOAD, PROCESSING_DATE)
print("✓ Execution steps generated — Attempts=1")
print("  TaskID=1 (config, DONE on Attempts=0) → skipped")
print("  All other tasks → new NQUE rows at Attempts=1")

# COMMAND ----------

# DBTITLE 1,View Pending Tasks — Run 2 (TaskID=1 absent — already done)
# TaskID=1 does not appear because it completed successfully on Attempts=0.
# The orchestrator (ADF ForEach / Databricks Workflow) only sees non-DONE tasks.
monitor.get_pending_tasks(EXECUTION_ID_2, PROJECT_CODE, PROCESS_LOAD, PROCESSING_DATE).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Complete the Retry Run

# COMMAND ----------

# DBTITLE 1,Run Initiation Task — Run 2 (Attempts=1)
with monitor.task(
    EXECUTION_ID_2, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 0,
    workflow_id     = 0,
    sequence_id     = 0,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "Retry run initiated",
):
    pass

print("✓ Initiation task completed (Attempts=1)")

# COMMAND ----------

# DBTITLE 1,Retry UK Employee Load (TaskID=2, Attempts=1) — Succeeds
# start_task auto-detects Attempts=1 from the NQUE row — no manual tracking needed.
with monitor.task(
    EXECUTION_ID_2, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 2,
    workflow_id     = 1,
    sequence_id     = 2,
    processing_date = PROCESSING_DATE,
    source_type     = "ADF_PIPELINE",
    log_message     = "UK employees loaded from SAP HR: 4,821 records (delta since last watermark)",
):
    import time
    time.sleep(1)

print("✓ UK employee load succeeded on retry — DELTA_DATE watermark auto-advanced")

# COMMAND ----------

# DBTITLE 1,US Employee Load (TaskID=3, Attempts=1)
with monitor.task(
    EXECUTION_ID_2, PROJECT_CODE, PROCESS_LOAD,
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

# DBTITLE 1,India Employee Load (TaskID=4, Attempts=1, non-mandatory)
with monitor.task(
    EXECUTION_ID_2, PROJECT_CODE, PROCESS_LOAD,
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

# DBTITLE 1,Org Structure Feed (TaskID=5, Attempts=1, non-mandatory)
with monitor.task(
    EXECUTION_ID_2, PROJECT_CODE, PROCESS_LOAD,
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

# DBTITLE 1,Verify Watermarks Auto-Advanced (SequenceID=2 tasks, Attempts=1)
spark.sql(f"""
    SELECT ParameterName, ParameterType, ActiveValue, ValueDateTime
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_watermarks`
    WHERE ProjectCode = 'HR'
      AND ProcessLoad = 'EMPLOYEE_MASTER'
    ORDER BY ParameterName
""").display()

# COMMAND ----------

# DBTITLE 1,Process Employee Dimensions (TaskID=6, Attempts=1)
with monitor.task(
    EXECUTION_ID_2, PROJECT_CODE, PROCESS_LOAD,
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

# DBTITLE 1,Apply HR Business Rules (TaskID=7, Attempts=1, non-mandatory)
with monitor.task(
    EXECUTION_ID_2, PROJECT_CODE, PROCESS_LOAD,
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

# DBTITLE 1,Build Employee Analytics Mart (TaskID=8, Attempts=1)
with monitor.task(
    EXECUTION_ID_2, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 8,
    workflow_id     = 2,
    sequence_id     = 6,
    processing_date = PROCESSING_DATE,
    source_type     = "DBX_NOTEBOOK",
    log_message     = "Analytics mart refreshed: headcount 8,268 | attrition rate 4.2% | 3 regions",
):
    import time
    time.sleep(1)

print("✓ Employee analytics mart built — retry run complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — Final Status Queries

# COMMAND ----------

# DBTITLE 1,Task Detail — Retry Run Complete (Run 2)
# Shows all 8 tasks that ran in Run 2 at Attempts=1.
# TaskID=1 (config) is absent — it was DONE on Run 1 and not re-inserted.
monitor.get_status(PROJECT_CODE, PROCESS_LOAD, execution_id=EXECUTION_ID_2).display()

# COMMAND ----------

# DBTITLE 1,Full Execution History — Both Runs on This Date
# Shows Attempts=0 (Run 1) and Attempts=1 (Run 2) side by side.
# TaskID=1 appears once (Attempts=0, DONE). TaskID=2 appears twice (FAIL then DONE).
# ORDER BY Attempts groups each run's tasks chronologically (0 = first run, 1 = retry);
# within each run tasks are in WorkFlowID → SequenceID → TaskID execution order.
# ExecutionID is a UUID — it has no chronological meaning so do not ORDER BY it.
spark.sql(f"""
    SELECT ExecutionID, Attempts, WorkFlowID, SequenceID, SequenceCode, TaskID, TaskName,
           Status, DurationSeconds, LogMessage
    FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_taskDetail`
    WHERE ProjectCode = 'HR'
      AND ProcessLoad = 'EMPLOYEE_MASTER'
      AND ProcessingDate = '{PROCESSING_DATE}'
    ORDER BY Attempts, WorkFlowID, SequenceID, TaskID
""").display()

# COMMAND ----------

# DBTITLE 1,Run Summary — Cross-Process Dashboard
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_processStatus`
    WHERE ProcessingDate = '{PROCESSING_DATE}'
    ORDER BY ProjectCode, ProcessLoad
""").display()

# COMMAND ----------

# DBTITLE 1,Run Summary View — All Executions on This Date
spark.sql(f"""
    SELECT * FROM `{MY_CATALOG}`.`{ETL_SCHEMA}`.`v_runSummary`
    WHERE ProcessingDate = '{PROCESSING_DATE}'
    ORDER BY ProjectCode, ProcessLoad
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Day Replay via status_reset
# MAGIC
# MAGIC `status_reset` resets **DONE → RQUE** — use this when a completed day needs to be
# MAGIC **fully re-processed** (e.g. data quality issue found after the run finished).
# MAGIC
# MAGIC This is **not** the failure-retry path. For failure retry: use a new ExecutionID
# MAGIC and call `generate_execution_steps` — the framework handles the rest.

# COMMAND ----------

# DBTITLE 1,Reset Entire Day for Replay (DONE → RQUE, all tasks on this date)
# Resets all tasks across all executions for this processing date back to RQUE.
monitor.status_reset(PROJECT_CODE, PROCESS_LOAD, processing_date=PROCESSING_DATE)
print(f"✓ All DONE tasks for {PROCESSING_DATE} reset to RQUE — ready for full day replay")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 — Processing Mode Examples

# COMMAND ----------

# DBTITLE 1,Switch to Bulk (Full Reload) Mode
# Sets all DELTA_DATE watermarks to NULL → next run performs a full reload from source.
monitor.set_processing_mode(PROJECT_CODE, PROCESS_LOAD, is_bulk_mode=True)
print("✓ Bulk mode enabled — next run will perform a full reload for all delta tasks")

# COMMAND ----------

# DBTITLE 1,Switch to Historic Rerun Mode
monitor.set_processing_mode(
    PROJECT_CODE, PROCESS_LOAD,
    is_historic_mode = True,
    processing_date  = "2026-03-01",
)
print("✓ Historic mode enabled — SYSDT = 2026-03-01")

# COMMAND ----------

# DBTITLE 1,Restore Live Mode (default)
monitor.set_processing_mode(PROJECT_CODE, PROCESS_LOAD)
print("✓ Live mode restored")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 — ADF-Style: Explicit start_task / end_task / fail_task
# MAGIC
# MAGIC When ADF orchestrates tasks it cannot use the Python `with monitor.task()` context manager
# MAGIC — each step is a separate ADF pipeline activity.  Instead, three lightweight utility notebooks
# MAGIC are wired to ADF's **On Success** / **On Failure** dependency edges:
# MAGIC
# MAGIC | Method | ADF utility notebook | Triggered by |
# MAGIC |--------|---------------------|-------------|
# MAGIC | `start_task()` | `etl_start_task.py` | Always — first activity in the task chain |
# MAGIC | `end_task()` | `etl_end_task.py` | **On Success** edge from the work activity |
# MAGIC | `fail_task()` | `etl_fail_task.py` | **On Failure** edge from the work activity |
# MAGIC
# MAGIC ADF itself knows whether the work activity succeeded or failed and routes accordingly.
# MAGIC The `Attempts` value is returned by `get_pending_tasks()` and flows through `item().Attempts`
# MAGIC in ADF ForEach, so every utility notebook receives the correct attempt number as a widget.
# MAGIC
# MAGIC The method names mirror the original SQL Server stored procedure split
# MAGIC (`p_ETLProcessingStatusUpdate` for DONE and for FAIL) — `start_task`, `end_task`, `fail_task`
# MAGIC are the standard names; no aliases needed.

# COMMAND ----------

# DBTITLE 1,Generate Fresh ExecutionID for ADF-Style Demo (new processing date)
# Use a new date to keep this demo independent of the retry scenario above.
PROCESSING_DATE_ADF = "2026-04-14"
EXECUTION_ID_ADF = ETLMonitorFramework.generate_execution_id()

print(f"ADF demo Execution ID : {EXECUTION_ID_ADF}")
print(f"ADF demo date         : {PROCESSING_DATE_ADF}")

monitor.generate_execution_steps(EXECUTION_ID_ADF, PROJECT_CODE, PROCESS_LOAD, PROCESSING_DATE_ADF)
print("✓ Execution steps generated — Attempts=0, all 9 tasks NQUE")

# COMMAND ----------

# DBTITLE 1,View Pending Tasks — note the Attempts column ADF ForEach passes as a widget
# In ADF ForEach: each item() row is one task.
#   item().Attempts → passed as widget to etl_start_task.py, etl_end_task.py, etl_fail_task.py
#   item().WatermarkValue → passed as ADF expression parameter to the Copy / Notebook activity
#   item().FullFileName + item().InFilePath → combined for file-based source tasks
pending_adf = monitor.get_pending_tasks(EXECUTION_ID_ADF, PROJECT_CODE, PROCESS_LOAD, PROCESSING_DATE_ADF)
pending_adf.display()

# COMMAND ----------

# DBTITLE 1,ADF path — Initiation task: start_task → end_task (On Success)
# etl_start_task.py receives execution_id, project_code, process_load,
#   task_id, workflow_id, sequence_id, processing_date, source_type as widgets.
# etl_end_task.py additionally receives log_message, log_type.
#
# start_task() auto-detects Attempts from the NQUE/RQUE row — no need to pass it.
# end_task() requires attempts so the WHERE clause hits the right row.
# In ADF ForEach, item().Attempts supplies this to both utility notebooks.
#
# Optional timing parameters:
#   timestamp  — optional ISO timestamp string, same parameter name across all three methods.
#                start_task: used as StartTime.
#                end_task / fail_task: used as EndTime; DurationSeconds = StartTime → timestamp.
#                Pass the actual compute time from the DBX job run or ADF pipeline activity
#                (e.g. pipeline().TriggerTime, job run start/end from the Runs API).
#                When omitted, current_timestamp() at the moment of the call is used — existing
#                behaviour is unchanged.

# start_task() and end_task() / fail_task() each auto-detect Attempts from the live row —
# no caller tracking needed. In ADF ForEach, item().Attempts (from get_pending_tasks()) can
# be passed explicitly to skip the lookup, but it is never required.

monitor.start_task(
    EXECUTION_ID_ADF, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 0,
    workflow_id     = 0,
    sequence_id     = 0,
    processing_date = PROCESSING_DATE_ADF,
    source_type     = "DBX_NOTEBOOK",
    # timestamp     = "2026-04-14T08:30:00",  # optional: actual compute start (DBX/ADF)
)
print("→ etl_start_task.py called — task is now in-progress")

# Simulate the actual work here (ADF activity runs between start and end utility notebooks)
import time; time.sleep(1)

# On Success edge: ADF calls etl_end_task.py → monitor.end_task(status="DONE")
monitor.end_task(
    EXECUTION_ID_ADF, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 0,
    workflow_id     = 0,
    sequence_id     = 0,
    processing_date = PROCESSING_DATE_ADF,
    status          = "DONE",                       # default — safe to omit, shown for clarity
    log_message     = "Initiation confirmed by ADF pipeline activity",
    # timestamp     = "2026-04-14T08:30:05",  # optional: actual compute end (DBX/ADF)
)
print("✓ etl_end_task.py called — Initiation task DONE")

# COMMAND ----------

# DBTITLE 1,ADF path — Config task: start_task → fail_task (On Failure)
# Simulate a work activity that fails in ADF.
# On Failure edge: ADF calls etl_fail_task.py → monitor.fail_task(log_message=...)
# fail_task() is a convenience wrapper around end_task(status="FAIL") — the names differ
# only so utility notebook authors don't need to know the status string.

monitor.start_task(
    EXECUTION_ID_ADF, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 1,
    workflow_id     = 1,
    sequence_id     = 1,
    processing_date = PROCESSING_DATE_ADF,
    source_type     = "DBX_NOTEBOOK",
)
print("→ etl_start_task.py called — Config Load task is now in-progress")

# On Failure edge: ADF calls etl_fail_task.py
monitor.fail_task(
    EXECUTION_ID_ADF, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 1,
    workflow_id     = 1,
    sequence_id     = 1,
    processing_date = PROCESSING_DATE_ADF,
    log_message     = "Source config database unreachable — connection refused (ADF activity error)",
)
print("✗ etl_fail_task.py called — Config Load task FAIL; LOAD_GO reset to NQUE")

# COMMAND ----------

# DBTITLE 1,ADF path — UK Load task: start_task → end_task (On Success, ADF_PIPELINE type)
# This simulates an ADF Copy Activity (or Dataflow) that succeeds.
# source_type="ADF_PIPELINE" records that the activity was an ADF pipeline — not a DBX notebook.

monitor.start_task(
    EXECUTION_ID_ADF, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 2,
    workflow_id     = 1,
    sequence_id     = 2,
    processing_date = PROCESSING_DATE_ADF,
    source_type     = "ADF_PIPELINE",
)
print("→ etl_start_task.py called — UK Load task in-progress (ADF Copy Activity running)")

import time; time.sleep(1)

monitor.end_task(
    EXECUTION_ID_ADF, PROJECT_CODE, PROCESS_LOAD,
    task_id         = 2,
    workflow_id     = 1,
    sequence_id     = 2,
    processing_date = PROCESSING_DATE_ADF,
    log_message     = "UK employees loaded from SAP HR via ADF Copy Activity: 4,821 rows",
)
print("✓ etl_end_task.py called — UK Load task DONE; DELTA_DATE watermark auto-advanced")

# COMMAND ----------

# DBTITLE 1,Status After ADF Demo — explicit calls produce the same result as context manager
# TaskID=0 (Initiation) → DONE via start_task + end_task
# TaskID=1 (Config)     → FAIL via start_task + fail_task
# TaskID=2 (UK Load)    → DONE via start_task + end_task
# TaskID=3–8            → NQUE (not yet started)
monitor.get_status(PROJECT_CODE, PROCESS_LOAD, execution_id=EXECUTION_ID_ADF).display()

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
