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
    └── generate_execution_steps(...)  → INSERT NQUE rows; Attempts-aware (see below)
    └── get_pending_tasks(...)         → two modes (mirrors original p_ETLProcessingSteps):
                                         · Orchestration (no task_id): non-DONE + IsActive=TRUE
                                           + ForceSkip=FALSE tasks; re-joins ETLconfigTasks each call
                                         · Per-task worker (task_id supplied): always 1 row;
                                           Status='NULL' = deactivated / ForceSkip / DONE / not generated
    └── task(...)                      → self-guarding context manager; NQUE → DONE/FAIL
                                         yields _TaskGuard(active, status)
    └── task_status(...)               → returns 'NQUE'/'RQUE'/'FAIL'/'NULL'
    └── skip_task(...)                 → sets ForceSkip=TRUE for this task in this run only
    └── unskip_task(...)               → clears ForceSkip back to FALSE within same execution
    └── start_task / end_task / fail_task  (optional ISO timestamp override)
    └── advance_watermark(...)         → manual DELTA_ID advance (KNOWN LIMITATION)
    └── get_active_watermark(...)      → returns typed watermark value
    └── get_status(...)                → task detail or summary rollup
    └── status_reset(...)              → reset DONE → RQUE for day replay (NOT for failure retry)
    └── set_processing_mode(...)       → bulk / historic / live mode
    └── generate_execution_id()        → static UUID generator
    └── sample_usage(spark)            → extracts bundled sample notebooks to Workspace

Tables (etl schema, ETL prefix + PascalCase):
    ETLconfigSequence      [FRAMEWORK-MANAGED — 7 built-in stages, auto-seeded]
    ETLconfigProcess       [USER-MANAGED — process / domain registry]
    ETLconfigTasks         [USER-MANAGED — task catalogue per process]
    ETLconfigParameters    [USER-MANAGED — delta watermarks + config flags]
    ETLProcessingSteps     [RESULTS — per-task live execution log, mutable]
    ETLsysLogs             [RESULTS — raw run receipts, append-only]

Views (v_ prefix):
    v_processStatus  v_runSummary  v_taskDetail
    v_mandatoryBlockers  v_currentFailures  v_watermarks
```

---

## CRITICAL — No DEFAULT expressions in DDL

**Never add `DEFAULT <expr>` to any column in `DDL_STATEMENTS`.**

`DEFAULT current_timestamp()` / `DEFAULT current_user()` / `DEFAULT TRUE` etc. require the
`delta.feature.allowColumnDefaults` table property — not guaranteed across all DBR versions.
This causes `WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED` on table creation.

**All default values are passed explicitly in the Python INSERT/MERGE statements instead.**

---

## Naming conventions — MUST follow exactly

### Columns — PascalCase, original SQL Server names — NEVER rename
```
TaskID          WorkFlowID (capital F)    Attempts (plural, not Attempt)
TaskMandatory   SequenceID                SourceSystemCode
ProcessingDate  ExecutionID               LogMessage
FileNameMask    FileExtension             InFilePath     OutFilePath
FullFileName    WatermarkType             WatermarkValue
```

### Audit columns — ALL user-managed config tables have all four
```
CreatedOn      TIMESTAMP
CreatedBy      STRING
LastUpdatedOn  TIMESTAMP
LastUpdatedBy  STRING
```

---

## Status values — exactly these four strings

| Value | Meaning |
|-------|---------|
| `NQUE` | New Queue — task created, first attempt, awaiting execution |
| `RQUE` | Re-Queue — reset from FAIL, retry attempt queued |
| `DONE` | Completed successfully |
| `FAIL` | Failed — awaiting retry or investigation |

State machine: `NQUE → DONE` or `NQUE → FAIL`

Retry path: use a **new ExecutionID** (mirrors ADF re-trigger with new RunID):
- `generate_execution_steps()` increments Attempts, skips DONE tasks, inserts NQUE rows for FAIL/NQUE tasks only.

Day replay: `status_reset()` resets DONE → RQUE to re-process an already-completed date. This is NOT for failure retry.

---

## WorkFlowID semantics

| WorkFlowID | Meaning |
|-----------|---------|
| 0 | Initiation task — always `TaskID=0`, `SequenceID=0`; one per process; overall run status indicator. Reset to NQUE on **any** task FAIL. |
| 1 | First workflow pass (main load) |
| 2 | Second pass (enrichment / additional fields) |
| N | Nth iteration over the same data with a different scope |

## Sequence stages (FRAMEWORK-MANAGED — auto-seeded by `setup()`)

| SequenceID | SequenceCode | Description |
|-----------|-------------|-------------|
| 0 | `LOAD_GO` | Initiating ETL Processing |
| 1 | `LOAD_DB_CONFIG` | Load Configuration Data from source |
| 2 | `LOAD_DB_TRAN` | Load Transactional Data from source (staging) |
| 3 | `LOAD_DIM` | Process Master Data — validate staged dimensions |
| 4 | `LOAD_TRAN` | Process Transactional Data — validate staged transactions |
| 5 | `PRE_PROCESS` | Functional Logic — business logic and derivations |
| 6 | `PROCESS_DATA` | Core Data Transformation — output / data mart tables |

**Custom stages: `SequenceID >= 10`. Framework reserves 0–9.**

All active tasks sharing `(WorkFlowID, SequenceID)` for a process are **intended to run in parallel** — ADF ForEach / Databricks Workflow handles the actual fan-out.

---

## Enterprise multi-org design

A single deployed instance (one catalog + `etl` schema) can serve an entire enterprise via the four-level hierarchy:

```
Unity Catalog          ← environment / platform isolation (DEV / UAT / PROD)
  ETLOrganisation      ← subsidiary / division / delivery unit / client entity
    ETLconfigProject   ← programme / department / project within an org
      ETLconfigProcess (ProjectCode + ProcessLoad)
                       ← individual data workstream within a project
```

The `OrganisationCode → ProjectCode → ProcessLoad` composite key ensures complete isolation between entities without needing separate schemas or catalogs.

Use a **separate Unity Catalog** (not just separate org rows) only when hard access control boundaries are required by regulation or contract.

---

## ParameterType values — exactly these four

| Value | Active column | Auto-advance on DONE? |
|-------|--------------|----------------------|
| `DELTA_DATE` | `ValueDateTime` | Yes — set to task `StartTime` |
| `DELTA_ID` | `ValueINT` | No — call `advance_watermark()` explicitly (KNOWN LIMITATION) |
| `FLAG` | `ValueBIT` | No |
| `SYSTEM` | `ValueDateTime` | No — `set_processing_mode()` only |

**DELTA_ID KNOWN LIMITATION:** Framework cannot auto-detect max integer ID from source. Developer must call `advance_watermark()` after load.

---

## Task exclusion — IsActive vs ForceSkip

| | `ETLconfigTasks.IsActive` | `ETLProcessingSteps.ForceSkip` |
|---|---|---|
| Scope | **Permanent** — all future runs | **One run only** — this ExecutionID only |
| Carries forward to retry? | Yes | No |
| Cleared by `status_reset()`? | No | **Yes** |
| Use case | Task retired or permanently disabled | Exclude from this specific run (upstream not ready) |

Developer contract:
```python
with monitor.task(exec_id, PROJECT, PROCESS,
                  task_id=4, workflow_id=1, sequence_id=2,
                  processing_date=DATE) as t:
    if t.active:
        actual_work()
    else:
        print(f"→ skipped ({t.status})")  # optional
```

---

## File-based source tasks

### ETLconfigTasks file columns
```
FileNameMask   STRING   — base filename without date suffix (e.g. payroll_uk)
FileExtension  STRING   — e.g. '.csv', '.xlsx' (include the dot)
InFilePath     STRING   — ADLS/storage base folder
OutFilePath    STRING   — output/processed folder (NULL if not needed)
```
`NULL FileNameMask` = non-file task; all four columns stay NULL.

### FullFileName date suffix by LoadFrequency
| LoadFrequency | Format | Example (2026-04-09) |
|---|---|---|
| `D` | `_yyyyMMdd` | `payroll_uk_20260409.csv` |
| `M` | `_yyyyMM`   | `payroll_uk_202604.csv` |
| `Y` | `_yyyy`     | `payroll_uk_2026.csv` |

### Period-aware NOT EXISTS in `generate_execution_steps()`
| LoadFrequency | Skip condition |
|---|---|
| `M` | DONE on **any** date in same calendar YYYYMM — monthly file loaded Apr 5 is NOT re-inserted Apr 6+ |
| `Y` | DONE on **any** date in same calendar YYYY |
| `D` / other | DONE on same ProcessingDate |

**Force reload:** `status_reset()` DONE → RQUE removes the DONE guard → next `generate_execution_steps()` inserts NQUE.

---

## Snapshot columns in ETLProcessingSteps

Copied from config tables at `generate_execution_steps()` time so history stays accurate even if task catalogue changes later:
- `TaskName`, `SequenceCode`, `TaskMandatory`, `SourceSystemCode`
- `FullFileName`, `InFilePath`, `OutFilePath` — also snapshotted; `FullFileName` is **computed** (not just copied) from `FileNameMask` + date suffix + `FileExtension`

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
        pass   # your notebook logic
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

# File-based task (monthly payroll drop)
monitor.register_task(
    "CORP", "PAYROLL_MONTHLY", task_id=1, workflow_id=1, sequence_id=2,
    task_name="Load Payroll File UK", source_type="DBX_NOTEBOOK",
    source_system_code="LoadPayrollUK", task_mandatory=True,
    load_frequency="M",
    file_name_mask="payroll_uk", file_extension=".csv",
    in_file_path="abfss://raw@store.dfs.core.windows.net/payroll/uk/monthly/",
)
```

---

## Register watermark parameters

```python
monitor.register_parameter("CORP", "HR_DAILY", "LoadEmployees", "DELTA_DATE",
                            description="Last loaded employee timestamp")
monitor.register_parameter("CORP", "HR_DAILY", "LoadEmployeesByID", "DELTA_ID",
                            value_int=0)   # 0 = bulk mode start
monitor.register_parameter("CORP", "HR_DAILY", "IsPartialLoad", "FLAG", value_bit=False)
monitor.register_parameter("CORP", "HR_DAILY", "SYSDT", "SYSTEM")
```

---

## Status queries

```sql
SELECT * FROM `<catalog>`.`etl`.`v_processStatus`     WHERE ProcessingDate = current_date();
SELECT * FROM `<catalog>`.`etl`.`v_taskDetail`         WHERE ExecutionID = '<exec_id>';
SELECT * FROM `<catalog>`.`etl`.`v_currentFailures`    WHERE ProcessingDate = current_date();
SELECT * FROM `<catalog>`.`etl`.`v_mandatoryBlockers`  WHERE ExecutionID = '<exec_id>';
SELECT * FROM `<catalog>`.`etl`.`v_watermarks`         WHERE ProjectCode = 'CORP';
```

---

## Failure retry — new ExecutionID

```python
exec_id_2 = ETLMonitorFramework.generate_execution_id()
monitor.generate_execution_steps(exec_id_2, "CORP", "HR_DAILY", "2026-04-09")
# Attempts=1; DONE tasks from exec_id skipped; FAIL/NQUE tasks re-inserted
pending = monitor.get_pending_tasks(exec_id_2, "CORP", "HR_DAILY", "2026-04-09")
```

---

## ADF integration

`v_watermarks.ActiveValue` is a resolved STRING for ADF Lookup:
```sql
SELECT ActiveValue FROM `<catalog>`.`etl`.`v_watermarks`
WHERE ProjectCode='CORP' AND ProcessLoad='HR_DAILY' AND ParameterName='LoadEmployees'
```

ADF Copy Activity source expression:
```
@concat('SELECT * FROM dbo.Employees WHERE ModifiedDate > ''',
        activity('GetWatermark').output.firstRow.ActiveValue, '''')
```

ADF utility notebooks share the same widget set: `execution_id`, `project_code`, `process_load`,
`task_id`, `workflow_id`, `sequence_id`, `processing_date`, `source_type`, `attempts`,
`log_message`, `log_type`, `log_code`, `timestamp`.  
The `timestamp` parameter is an optional ISO string — defaults to `current_timestamp()`.

`WatermarkValue` in `get_pending_tasks()` output replaces the original `#DELTAPARAMETER#` substitution. ADF uses it in `@concat()` expressions. `FullFileName + InFilePath` tell ADF where to find the input file.

### ADF ForEach integration
```
WatermarkValue → @concat('SELECT * FROM src WHERE ModifiedDate > ''', item().WatermarkValue, '''')
FullFileName   → @concat(item().InFilePath, item().FullFileName)
```

---

## Stored procedure equivalence (SQL Server migration reference)

| Original stored procedure | Python method | Notes |
|--------------------------|--------------|-------|
| `p_ETLProcessingSteps` (GenerateMode=1) | `generate_execution_steps()` | Attempts-aware; period-aware NOT EXISTS; FullFileName computed at generate time |
| `p_ETLProcessingSteps` (GenerateMode=0, per-task) | `get_pending_tasks(..., task_id=N, ...)` | Always 1 row; `Status='NULL'` = skip. Replicates `FROM (SELECT 'NULL') LEFT JOIN … COALESCE` pattern |
| `p_ETLOrchestrationSteps` | `get_pending_tasks()` (no `task_id`) | Returns all non-DONE, IsActive=TRUE tasks; auto-generates on first call |
| `p_ETLProcessingStatusUpdate` | `end_task()` / `fail_task()` | Status + timing write-back; DELTA_DATE auto-advance on DONE |
| `p_ETLProcessingStatusGet` | `get_status()` | Summary or task-level detail |
| `p_ETLProcessingStatusReset` | `status_reset()` | DONE → RQUE for day replay |
| `p_ETLconfigProcessingMode` | `set_processing_mode()` | Historic / bulk / live mode |
| `#DELTAPARAMETER#` inline substitution | `v_watermarks.ActiveValue` | ADF reads as plain column, builds `@concat()` itself |

What was **not ported**: trigger/orchestration logic, ADF pipeline driver config tables (`ADFMain`, `ADFPipelines`), `ETLconfigNotifications` (replaced by Databricks SQL Alerts), T-SQL stored procedures.

---

## INSERT-ONLY MERGE pattern — all config writes

```sql
MERGE INTO `<catalog>`.`etl`.`ETLconfigTasks` AS tgt
USING (SELECT ...) AS src
ON  tgt.TaskID     = src.TaskID
AND tgt.WorkFlowID = src.WorkFlowID
AND COALESCE(tgt.ProjectCode,'') = COALESCE(src.ProjectCode,'')
AND COALESCE(tgt.ProcessLoad, '') = COALESCE(src.ProcessLoad, '')
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
```

Watermark value columns (`ValueDateTime`, `ValueINT`, `ValueBIT`) in `ETLconfigParameters` are **never overwritten on UPDATE** — use `advance_watermark()` or `set_processing_mode()`.

---

## Rules — always apply

1. **Databricks SQL only** — `CREATE TABLE IF NOT EXISTS`, `USING DELTA`, backtick FQNs, `current_timestamp()`, `current_user()`.
2. **No DEFAULT expressions in DDL** — causes `WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED`.
3. **Original SQL Server column names** — `WorkFlowID` (capital F), `Attempts` (plural), `TaskMandatory`. Never snake_case.
4. **`v_` prefix for all views** — `v_runSummary` not `vw_run_summary`.
5. **`<catalog>` placeholder in SQL** — never hardcode a real catalog name.
6. **Schema is always `etl`** unless the user overrides explicitly.
7. **Status values exactly:** `NQUE`, `RQUE`, `DONE`, `FAIL`.
8. **ParameterType values exactly:** `DELTA_DATE`, `DELTA_ID`, `FLAG`, `SYSTEM`.
9. **No triggering logic** — never add code that starts or schedules a job.
10. **COALESCE in MERGE ON clauses** for nullable string keys.
11. **DELTA_ID KNOWN LIMITATION** — developer must call `advance_watermark()` explicitly.
12. **All four audit columns** on every config table: `CreatedOn`, `CreatedBy`, `LastUpdatedOn`, `LastUpdatedBy`.
13. **Snapshot columns** — `TaskName`, `SequenceCode`, `TaskMandatory`, `SourceSystemCode`, `FullFileName`, `InFilePath`, `OutFilePath` are copied/computed into `ETLProcessingSteps` at `generate_execution_steps()` time. Never let callers manually insert `FullFileName`.
14. **Period-aware NOT EXISTS** — M/Y frequency tasks use cross-date DONE guard. `status_reset()` is the only authorised path to force a same-period reload.
15. **SequenceID 0–9 reserved** — custom stages use `>= 10`.

---

## Response format

- Lead with the code block (SQL or Python) — explanation after.
- Use `<catalog>` placeholder unless the user has given the real value.
- When instrumenting a notebook, always show both `setup()` and `monitor.task()` together.
- For status queries, default to `v_taskDetail` filtered by `ExecutionID`.
- For ADF watermark questions, always show `v_watermarks.ActiveValue` as the bridge.
- When a user asks about IDE errors on a SQL file, explain they are T-SQL linter false positives — the file is Databricks SQL.
