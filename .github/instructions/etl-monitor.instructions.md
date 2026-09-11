---
applyTo: "databricks-etl-monitor/**"
---

# ETL Monitor Framework — Copilot Instructions

You are working on the **ETL Monitoring Framework** in `databricks-etl-monitor/`.

Start by reading `databricks-etl-monitor/CLAUDE.md` — it contains the full naming conventions, architecture rules, and usage reference.

---

## Role

This framework tracks ADF pipelines, Databricks notebooks, Databricks jobs, and Dataflows. It does **not** trigger or orchestrate them — it only observes and records.

---

## Architecture

```
ETLMonitorFramework(spark, catalog, schema="etl")
    └── setup()                        → schema + tables + views + seed (idempotent)
    └── register_organisation(...)     → INSERT/UPDATE MERGE into ETLOrganisation
    └── register_project(...)          → INSERT/UPDATE MERGE into ETLconfigProject
    └── register_process(...)          → INSERT/UPDATE MERGE into ETLconfigProcess
    └── register_task(...)             → INSERT/UPDATE MERGE into ETLconfigTasks
    └── register_parameter(...)        → INSERT/UPDATE MERGE into ETLconfigParameters
    └── generate_execution_steps(...)  → INSERT NQUE rows; Attempts-aware
    └── get_pending_tasks(...)         → two modes:
                                          · Orchestration (no task_id): non-DONE + IsActive=TRUE tasks
                                          · Per-task (task_id supplied): always 1 row
    └── task(...)                      → self-guarding context manager; NQUE → DONE/FAIL
    └── task_status(...)               → returns 'NQUE'/'RQUE'/'FAIL'/'NULL'
    └── skip_task(...)                 → ForceSkip=TRUE for this task/run
    └── unskip_task(...)               → ForceSkip=FALSE within same execution
    └── start_task / end_task / fail_task (optional timestamp override)
    └── advance_watermark(...)         → manual DELTA_ID advance (KNOWN LIMITATION)
    └── get_active_watermark(...)      → returns typed watermark value
    └── get_status(...)                → task detail or summary rollup
    └── status_reset(...)              → reset DONE → RQUE for day replay
    └── set_processing_mode(...)       → bulk / historic / live mode
    └── generate_execution_id()        → static UUID generator

Tables (etl schema, ETL prefix + PascalCase):
    ETLconfigSequence      [FRAMEWORK-MANAGED — 7 stages]
    ETLconfigProcess       [USER-MANAGED]
    ETLconfigTasks         [USER-MANAGED]
    ETLconfigParameters    [USER-MANAGED]
    ETLProcessingSteps     [RESULTS — mutable]
    ETLsysLogs             [RESULTS — append-only]

Views (v_ prefix):
    v_processStatus  v_runSummary  v_taskDetail
    v_mandatoryBlockers  v_currentFailures  v_watermarks
```

---

## Instrument a notebook or job

```python
from etl_monitor import ETLMonitorFramework
monitor = ETLMonitorFramework(spark, catalog="<catalog>", schema="etl")
monitor.setup()   # idempotent

EXECUTION_ID  = dbutils.widgets.get("execution_id")
PROC_DATE     = dbutils.widgets.get("processing_date")
PROJECT_CODE  = dbutils.widgets.get("project_code")
PROCESS_LOAD  = dbutils.widgets.get("process_load")

with monitor.task(EXECUTION_ID, PROJECT_CODE, PROCESS_LOAD,
                  task_id=<task_id>,
                  workflow_id=<workflow_id>,
                  sequence_id=<sequence_id>,
                  processing_date=PROC_DATE,
                  source_type="DBX_NOTEBOOK",   # or DBX_JOB | ADF_PIPELINE | DATAFLOW
                  log_message="Optional success note") as t:
    if t.active:
        pass   # replace with notebook logic
```

---

## Register a process and tasks

```python
monitor.register_process(
    "CORP", "HR_DAILY",
    name="HR Daily Load",
    description="Daily employee and payroll ingestion",
    owner="HR Platform Team",
    load_frequency="D"
)

# Initiation task — always WorkFlowID=0, SequenceID=0, TaskID=0
monitor.register_task("CORP", "HR_DAILY", task_id=0, workflow_id=0, sequence_id=0,
                       task_name="Initiation", source_type="DBX_NOTEBOOK")

monitor.register_task("CORP", "HR_DAILY", task_id=1, workflow_id=1, sequence_id=2,
                       task_name="Load Employees", source_type="DBX_NOTEBOOK",
                       source_identifier="/Repos/project/load_employees",
                       source_system_code="LoadEmployees", task_mandatory=True)
```

---

## Register watermark parameters

```python
# DELTA_DATE — auto-advanced on task DONE
monitor.register_parameter("CORP", "HR_DAILY", "LoadEmployees", "DELTA_DATE",
                            description="Last loaded employee timestamp")

# DELTA_ID — developer must call advance_watermark() manually (KNOWN LIMITATION)
monitor.register_parameter("CORP", "HR_DAILY", "LoadEmployeesByID", "DELTA_ID",
                            value_int=0)

# FLAG / SYSTEM
monitor.register_parameter("CORP", "HR_DAILY", "IsPartialLoad", "FLAG", value_bit=False)
monitor.register_parameter("CORP", "HR_DAILY", "SYSDT", "SYSTEM")
```

**ParameterType values:** `DELTA_DATE`, `DELTA_ID`, `FLAG`, `SYSTEM` — no others.

---

## ForceSkip — run-level task exclusion

```python
# Skip for this run only (task remains active for future runs)
monitor.skip_task(execution_id, PROJECT_CODE, PROCESS_LOAD,
                  task_id=4, workflow_id=1, sequence_id=2, processing_date=PROC_DATE)

# Mid-run re-enable
monitor.unskip_task(execution_id, PROJECT_CODE, PROCESS_LOAD,
                    task_id=4, workflow_id=1, sequence_id=2, processing_date=PROC_DATE)
```

**ForceSkip vs IsActive:** `ForceSkip=TRUE` is run-level (this ExecutionID only); `IsActive=FALSE` is permanent (all future runs).

---

## Status queries

```sql
SELECT * FROM `<catalog>`.`etl`.`v_processStatus` WHERE ProcessingDate = current_date();
SELECT * FROM `<catalog>`.`etl`.`v_taskDetail`    WHERE ExecutionID = '<exec_id>';
SELECT * FROM `<catalog>`.`etl`.`v_currentFailures` WHERE ProcessingDate = current_date();
SELECT * FROM `<catalog>`.`etl`.`v_mandatoryBlockers` WHERE ExecutionID = '<exec_id>';
SELECT * FROM `<catalog>`.`etl`.`v_watermarks`    WHERE ProjectCode = 'CORP';
```

---

## Failure retry — new ExecutionID

Retry always uses a **new ExecutionID**. `generate_execution_steps` auto-increments Attempts and skips tasks already DONE.

```python
exec_id_2 = ETLMonitorFramework.generate_execution_id()
monitor.generate_execution_steps(exec_id_2, "CORP", "HR_DAILY", "2026-04-09")
pending = monitor.get_pending_tasks(exec_id_2, "CORP", "HR_DAILY", "2026-04-09")
```

---

## Day replay

```python
monitor.status_reset("CORP", "HR_DAILY", processing_date="2026-04-09")  # full date replay
```

Use `status_reset` only when re-processing a date that already completed successfully. For failure retry, use a new ExecutionID instead.

---

## Rules — always apply

- **Databricks SQL only** — `CREATE TABLE IF NOT EXISTS`, `USING DELTA`, `current_user()`, `current_timestamp()`, backtick FQNs.
- **Original SQL Server table/column names** — `ETLconfigSequence`, `ETLProcessingSteps`, `TaskID`, `WorkFlowID` (capital F), `Attempts` (plural). Never revert to snake_case.
- **`v_` prefix for all views** — `v_runSummary` not `vw_run_summary`.
- **`<catalog>` placeholder** in SQL — never hardcode a real catalog name.
- **Schema is always `etl`** unless the user overrides explicitly.
- **Status values are exactly:** `NQUE`, `RQUE`, `DONE`, `FAIL` — no others.
- **No triggering logic** — never add code that starts or schedules a job.
- **COALESCE in MERGE ON clauses** for nullable string keys.
- **DELTA_ID KNOWN LIMITATION** — developer must call `advance_watermark()` for ID-based watermarks.

---

## Response format

- Lead with the code block (SQL or Python) — explanation after.
- Use `<catalog>` placeholder unless the user has given the real value.
- When instrumenting a notebook, always show both `setup()` and `monitor.task()` together.
- For status queries, default to `v_taskDetail` filtered by `ExecutionID`.
- For ADF watermark questions, always show `v_watermarks.ActiveValue` as the bridge.
