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

```
NQUE    Not queued (waiting)
RQUE    Retry queued (after reset from FAIL)
DONE    Completed successfully
FAIL    Failed
```

State machine: `NQUE → DONE | NQUE → FAIL → RQUE → DONE`

---

## ParameterType values — exactly these four

| Value | Active column | Auto-advance on DONE? |
|-------|---------------|----------------------|
| `DELTA_DATE` | `ValueDateTime` | Yes — to task `StartTime` |
| `DELTA_ID` | `ValueINT` | No — call `advance_watermark()` manually |
| `FLAG` | `ValueBIT` | No |
| `SYSTEM` | `ValueDateTime` | No — via `set_processing_mode()` only |

**KNOWN LIMITATION — DELTA_ID:** Framework cannot auto-detect the max integer ID from
the source dataset. Developer must call `advance_watermark()` explicitly after load.

---

## WorkFlowID semantics

```
0  Initiation task — overall run status marker; one per process; always TaskID=0, SequenceID=0
1  First workflow pass (main load)
2  Second pass (enrichment / second data iteration)
N  Nth iteration over same data with different scope
```

## SequenceID parallelism

All active tasks sharing `(WorkFlowID, SequenceID)` for a process are **intended to run in parallel**.
The ADF ForEach / Databricks Workflow fan-out handles the actual parallelism.
Developer designs the fan-out accordingly.

## SequenceID ranges

```
0–9    Framework-reserved (built-in LOAD_GO → PROCESS_DATA stages)
≥ 10   Custom / project-specific stages
```

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
