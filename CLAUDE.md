# ETL Monitor Framework — Project Context

## What this folder is

An **open-source Python package** (`databricks-etl-monitor`) that provides metadata-driven
ETL process monitoring for Databricks / Delta Lake environments.

Ported from a SQL Server / ADF monitoring framework.  Deployed as a Python wheel and
consumed in Databricks notebooks.

Current version: see `pyproject.toml`.

---

## Files in this folder

| File / Path | Purpose |
|-------------|---------|
| `etl_monitor/framework.py` | `ETLMonitorFramework` class — main entry point |
| `etl_monitor/ddl_tables.py` | `DDL_STATEMENTS` dict + `TABLE_ORDER` — all table DDL |
| `etl_monitor/seed_data.py` | `SEQUENCE_SEED` — 7 built-in workflow stage definitions |
| `etl_monitor/__init__.py` | Package entry point — exposes `ETLMonitorFramework` |
| `etl_monitor/sample_usage/` | Sample notebooks bundled inside the package |
| `pyproject.toml` | Package metadata — PyPI name: `databricks-etl-monitor` |
| `build_and_publish.bat` | Build and upload to PyPI |
| `CHANGELOG.md` | Version history |

---

## Architecture

```
ETLMonitorFramework(spark, catalog, schema="etl")
    └── setup()                        → schema + tables + views + seed (idempotent)
    └── register_process(...)          → INSERT-ONLY MERGE into ETLconfigProcess
    └── register_task(...)             → INSERT-ONLY MERGE into ETLconfigTasks
    └── register_parameter(...)        → INSERT-ONLY MERGE into ETLconfigParameters
    └── generate_execution_steps(...)  → INSERT NQUE rows for all active tasks
    └── get_pending_tasks(...)         → non-DONE tasks; auto-generates on first call
    └── task(...)                      → context manager: NQUE → DONE/FAIL
    └── start_task / end_task / fail_task
    └── advance_watermark(...)         → manual DELTA_ID advance (KNOWN LIMITATION)
    └── get_active_watermark(...)      → returns typed watermark value
    └── get_status(...)                → task detail or summary rollup
    └── status_reset(...)              → reset FAIL → RQUE for retry
    └── set_processing_mode(...)       → bulk / historic / live mode
    └── generate_execution_id()        → static UUID generator
    └── sample_usage(spark)            → extracts bundled sample notebooks to Workspace
```

---

## Naming conventions — MUST follow exactly

### Schema
- **Default:** `schema="etl"` in constructor.
- **Never mix** with DQ framework's `dq` schema.

### Tables — ETL prefix + PascalCase

```
ETLconfigSequence      [FRAMEWORK-MANAGED — 7 built-in stages, auto-seeded]
ETLconfigProcess       [USER-MANAGED — process / domain registry]
ETLconfigTasks         [USER-MANAGED — task catalogue per process]
ETLconfigParameters    [USER-MANAGED — delta watermarks + config flags]
ETLProcessingSteps     [RESULTS — per-task live execution log, mutable]
ETLsysLogs             [RESULTS — raw run receipts, append-only]
```

### Views — `v_` prefix

```
v_processStatus        v_runSummary        v_taskDetail
v_mandatoryBlockers    v_currentFailures   v_watermarks
```

### Columns — PascalCase, original SQL Server names — NEVER rename
```
TaskID          WorkFlowID (capital F)    Attempts (plural, not Attempt)
TaskMandatory   SequenceID                SourceSystemCode
ProcessingDate  ExecutionID               LogMessage
```

### Audit columns — ALL user-managed config tables have all four
```
CreatedOn      TIMESTAMP    DEFAULT current_timestamp()
CreatedBy      STRING       DEFAULT current_user()
LastUpdatedOn  TIMESTAMP    DEFAULT current_timestamp()
LastUpdatedBy  STRING       DEFAULT current_user()
```

### FQN — always backtick-quoted
```python
def _fqn(self, name: str) -> str:
    if self.catalog:
        return f"`{self.catalog}`.`{self.schema}`.`{name}`"
    return f"`{self.schema}`.`{name}`"
```

---

## Status values — exactly these four strings

| Value | Full name | Meaning |
|-------|-----------|---------|
| `NQUE` | New Queue | Task created, first attempt, awaiting execution |
| `RQUE` | Re-Queue | Reset from FAIL, retry attempt queued |
| `DONE` | Done | Completed successfully |
| `FAIL` | Failed | Failed — awaiting retry or investigation |

State machine:
```
NQUE → DONE
NQUE → FAIL → RQUE → DONE
NQUE → FAIL → RQUE → FAIL → [manual status_reset()] → RQUE → DONE
```

---

## ParameterType values — exactly these four

| Value | Active column | Auto-advance on DONE? | Bulk mode |
|-------|--------------|----------------------|-----------|
| `DELTA_DATE` | `ValueDateTime` | Yes — set to task `StartTime` | `ValueDateTime = NULL` |
| `DELTA_ID` | `ValueINT` | No — call `advance_watermark()` | `ValueINT = 0` |
| `FLAG` | `ValueBIT` | No — read freely | Not applicable |
| `SYSTEM` | `ValueDateTime` | No — `set_processing_mode()` only | `NULL` = live date |

**KNOWN LIMITATION — DELTA_ID:** Framework cannot auto-detect the max integer ID from
the source dataset. Developer must call `advance_watermark()` explicitly after load.

**Future enhancement:** Accept an optional `new_delta_id` parameter in `end_task()` so the
developer can pass the value in a single call without a separate `advance_watermark()` call.

**Original framework note:** The original SQL Server framework used
`ParameterDescription LIKE 'Delta Date;%'` string matching to detect watermark type.
This was fragile and hard to query. The DBX version replaces it with an explicit
`ParameterType` column — one of exactly four values above.

---

## WorkFlowID semantics

| WorkFlowID | Meaning |
|-----------|---------|
| 0 | Initiation task — always `TaskID=0`, `SequenceID=0`; one per process; overall run status indicator. Reset to NQUE on any mandatory FAIL. |
| 1 | First workflow pass (main load) |
| 2 | Second pass (enrichment / additional fields / second data iteration) |
| N | Nth iteration over the same data with a different scope |

## Sequence stages (FRAMEWORK-MANAGED — auto-seeded by `setup()`)

| SequenceID | SequenceCode | Description | SortOrder |
|-----------|-------------|-------------|-----------|
| 0 | `LOAD_GO` | Initiating ETL Processing | 0 |
| 1 | `LOAD_DB_CONFIG` | Load Configuration Data from source | 1 |
| 2 | `LOAD_DB_TRAN` | Load Transactional Data from source (staging) | 2 |
| 3 | `LOAD_DIM` | Process Master Data — validate staged dimensions | 3 |
| 4 | `LOAD_TRAN` | Process Transactional Data — validate staged transactions | 4 |
| 5 | `PRE_PROCESS` | Functional Logic — business logic and derivations | 5 |
| 6 | `PROCESS_DATA` | Core Data Transformation — output / data mart tables | 6 |

Custom stages: `SequenceID >= 10`. Framework reserves 0–9.

## SequenceID parallelism

All active tasks sharing `(WorkFlowID, SequenceID)` for a process are **intended to run in parallel**.
The ADF ForEach / Databricks Workflow fan-out handles the actual parallelism.
Developer designs the fan-out accordingly.

## ProcessLoad scoping (enhancement over original SQL Server framework)

Original framework used `(ProjectCode, ParameterName)` as the parameter key. This caused
namespace collision when multiple processes under the same project share parameter names
(e.g. `CORP / HR_DAILY / LoadEmployees` and `CORP / FIN_MONTHLY / LoadEmployees`).

The DBX version adds `ProcessLoad` to the composite key across all user-managed tables.
HR_DAILY and FIN_MONTHLY are fully independent — their tasks, watermarks, and execution
histories do not interact.

## SequenceID ranges

```
0–9    Framework-reserved (built-in LOAD_GO → PROCESS_DATA stages)
≥ 10   Custom / project-specific stages
```

---

## Stored procedure equivalence (for teams migrating from SQL Server)

| Original stored procedure | Python method | Notes |
|--------------------------|--------------|-------|
| `p_ETLProcessingSteps` (GenerateMode=1) | `generate_execution_steps()` | INSERT NQUE rows for all active tasks |
| `p_ETLOrchestrationSteps` | `get_pending_tasks()` | Returns non-DONE tasks; auto-generates on first call |
| `p_ETLProcessingStatusUpdate` | `end_task()` / `fail_task()` | Status + timing write-back; DELTA_DATE auto-advance on DONE |
| `p_ETLProcessingStatusGet` | `get_status()` | Summary or task-level detail; `summary_mode=True` for rollup |
| `p_ETLProcessingStatusReset` | `status_reset()` | Bulk or specific task reset; always resets initiation row |
| `p_ETLconfigProcessingMode` | `set_processing_mode()` | Historic mode, live mode, bulk mode, specific param |

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

**Never use `WHEN MATCHED THEN UPDATE`** in config writes — preserves existing data.
**Always use `COALESCE`** for nullable string keys in `MERGE ON` conditions — never
`IS DISTINCT FROM` or `NOT (col <=> val)`.

---

## Key design patterns

### Pattern A — Composite execution key
`(ProcessingDate, ProjectCode, ProcessLoad, ExecutionID, WorkFlowID, TaskID, SequenceID, Attempts)`
Every row in `ETLProcessingSteps` is uniquely addressable for replay, retry, and historical comparison.

### Pattern B — INSERT-ONLY MERGE for config writes
All config upserts use `MERGE INTO ... WHEN NOT MATCHED THEN INSERT`.
Safe to re-run without overwriting existing user modifications.  Never add `WHEN MATCHED THEN UPDATE`
to config MERGE statements.

### Pattern C — COALESCE in MERGE ON clause
```sql
ON  COALESCE(tgt.ProjectCode,  '') = COALESCE(src.ProjectCode,  '')
AND COALESCE(tgt.ProcessLoad,  '') = COALESCE(src.ProcessLoad,  '')
AND COALESCE(tgt.ParameterName,'') = COALESCE(src.ParameterName,'')
```
Handles nullable string keys safely.  Never use `IS DISTINCT FROM` or `NOT (col <=> val)`.

### Pattern D — `_fqn()` helper
Identical to DQ framework pattern.  Backtick-quoting handles names with hyphens or reserved words.

### Pattern E — Context manager (`with monitor.task(...)`)
Writes NQUE at entry, DONE on clean exit, FAIL on exception.  UUID generated per run via
`ETLMonitorFramework.generate_execution_id()`.

### Pattern F — Snapshot columns in ETLProcessingSteps
`TaskName`, `SequenceCode`, `TaskMandatory`, `SourceSystemCode` copied from config tables
at `generate_execution_steps()` time.  History stays accurate even if the task catalogue
is later changed or tasks are deactivated.

---

## ADF integration

`v_watermarks.ActiveValue` is a resolved STRING suitable for ADF Lookup activity:

```sql
SELECT ActiveValue FROM `<catalog>`.`etl`.`v_watermarks`
WHERE ProjectCode='RETAIL' AND ProcessLoad='DAILY_LOAD' AND ParameterName='LoadProducts'
```

ADF Copy Activity source expression:
```
@concat('SELECT * FROM dbo.Products WHERE ModifiedDate > ''',
        activity('GetWatermark').output.firstRow.ActiveValue, '''')
```

### ADF write-back via utility notebooks
ADF calls a Databricks Notebook activity passing widget parameters.
Three lightweight utility notebooks are created per project (not shipped with this package):
- `etl_start_task.py` — widgets: `execution_id`, `project_code`, `process_load`, `task_id`,
  `workflow_id`, `sequence_id`, `processing_date`, `source_type` → calls `monitor.start_task(...)`.
- `etl_end_task.py` — same widgets + `log_message`, `log_type` → calls `monitor.end_task(...)`.
- `etl_fail_task.py` — same widgets + error details → calls `monitor.fail_task(...)`.

### ADF ForEach over pending tasks
ADF ForEach iterates `get_pending_tasks()` output.
Tasks sharing the same `SequenceID` are dispatched in parallel (ADF parallel ForEach).
After each SequenceID stage completes, ADF checks `v_mandatoryBlockers` before advancing.

---

## What was NOT ported

| Original | Reason not ported |
|----------|------------------|
| Trigger / orchestration logic | Out of scope — this framework observes only, never triggers |
| `#DELTAPARAMETER#` string substitution | Replaced by `v_watermarks.ActiveValue` |
| `ADFMain` / `ADFPipelines` / `ADFMetaData` | ADF pipeline driver config — not needed for monitoring |
| `ETLconfigNotifications` | Replace with Databricks SQL Alerts or Lakeview dashboards |
| T-SQL stored procedures | Replaced entirely by Python class methods |

---

## Key rules — always apply

1. **Databricks SQL syntax only** — `CREATE TABLE IF NOT EXISTS`, `USING DELTA`, backtick FQNs,
   `current_timestamp()`, `current_user()`, `GENERATED ALWAYS AS IDENTITY`.
2. **Original SQL Server column names** — `WorkFlowID` (capital F), `Attempts` (plural),
   `TaskMandatory`, `SequenceID`. Never revert to snake_case.
3. **`v_` prefix for all views** — `v_runSummary` not `vw_run_summary`.
4. **`<catalog>` placeholder in SQL** — never hardcode a real catalog name.
5. **Schema is always `etl`** unless the user overrides it explicitly.
6. **Status values are exactly:** `NQUE`, `RQUE`, `DONE`, `FAIL`.
7. **ParameterType values are exactly:** `DELTA_DATE`, `DELTA_ID`, `FLAG`, `SYSTEM`.
8. **No triggering logic** — framework only observes and records; never starts or schedules jobs.
9. **No PII or client/org names** in any generated code or SQL.
10. **COALESCE in MERGE ON clauses** for nullable string keys.
11. **DELTA_ID KNOWN LIMITATION** — always note that developer must call `advance_watermark()`.
12. **All four audit columns on every config table** — `CreatedOn`, `CreatedBy`,
    `LastUpdatedOn`, `LastUpdatedBy`.
13. **Snapshot columns** — `TaskName`, `SequenceCode`, `TaskMandatory`, `SourceSystemCode`
    are copied into `ETLProcessingSteps` at `generate_execution_steps()` time.
    History remains accurate even if the catalogue changes later.

---

## Quick usage reference

```python
from etl_monitor import ETLMonitorFramework

monitor = ETLMonitorFramework(spark, catalog="<catalog>", schema="etl")
monitor.setup()   # idempotent

# Register (once per process/task/parameter)
monitor.register_process("CORP", "HR_DAILY", name="HR Daily Load")
monitor.register_task("CORP", "HR_DAILY", task_id=1, workflow_id=1, sequence_id=2,
                      task_name="Load Employees", source_system_code="LoadEmployees")
monitor.register_parameter("CORP", "HR_DAILY", "LoadEmployees", "DELTA_DATE")

# Each run
exec_id = ETLMonitorFramework.generate_execution_id()
monitor.generate_execution_steps(exec_id, "CORP", "HR_DAILY", "2026-04-09")

with monitor.task(exec_id, "CORP", "HR_DAILY",
                  task_id=1, workflow_id=1, sequence_id=2,
                  processing_date="2026-04-09"):
    pass   # your notebook logic here
```

---

## Package build and publish

```bash
python -m build
build_and_publish.bat
```

Installed on clusters via:
```python
%pip install databricks-etl-monitor
```
