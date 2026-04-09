# Databricks ETL Monitor Framework

A metadata-driven ETL process monitoring framework for Databricks / Delta Lake.

Tracks ADF pipelines, Databricks notebooks, Databricks jobs, and Dataflows in a single
Unity Catalog schema — **without modifying how those jobs are triggered**.

Ported from a mature SQL Server / ADF ETL orchestration framework.  The monitoring and
tracking patterns have been reimplemented as Unity Catalog Delta tables and a self-creating
Python class, making them available to pure Databricks teams without a SQL Server dependency.

---

## Why this framework?

Most existing tools cover one platform or the other:

| Tool | What it covers | Gap |
|------|---------------|-----|
| ADF monitoring (Azure Portal) | ADF pipeline runs | No DBX notebooks, no task-level app log |
| Databricks job UI / API | DBX jobs only | No ADF pipelines, no cross-job correlation |
| Azure Monitor / Log Analytics | Raw platform logs | Not process-aware — no task catalogue, no watermarks |
| Apache Atlas / Unity Catalog lineage | Data lineage | Not execution-status oriented |
| Great Expectations / Soda | Data quality | Not ETL step tracking |
| Custom Delta audit tables | Project-specific | No standard schema, no Python SDK, no ADF bridge |

**What this framework adds:**

1. Single Delta schema tracks ADF pipelines, DBX notebooks, DBX jobs, and Dataflows in one place.
2. Task catalogue (`ETLconfigTasks`) with sequence/workflow ordering — not just raw run receipts.
3. Delta watermark registry (`ETLconfigParameters`) with typed parameters and ADF Lookup bridge.
4. Per-process scoping (`ProjectCode / ProcessLoad`) — HR_DAILY and FIN_MONTHLY tracked independently.
5. Snapshot columns in execution log — history stays accurate even when the task catalogue changes.
6. Self-creating Python class (`setup()`) — zero manual DDL for Databricks teams.
7. JDBC-queryable by SQL Server, ADF, and any ODBC tool — no Python SDK needed for consumption.

---

## Quick Start

```python
%pip install databricks-etl-monitor --upgrade --no-deps

from etl_monitor import ETLMonitorFramework

monitor = ETLMonitorFramework(spark, catalog="main", schema="etl")
monitor.setup()   # idempotent — creates 6 tables, 6 views, seeds sequence stages

# Register a domain process (once)
monitor.register_process("CORP", "HR_DAILY", name="HR Daily Load", owner="HR Team", load_frequency="D")

# Register tasks (once)
monitor.register_task("CORP", "HR_DAILY", task_id=0, workflow_id=0, sequence_id=0,
                      task_name="Initiation", source_type="DBX_NOTEBOOK")
monitor.register_task("CORP", "HR_DAILY", task_id=1, workflow_id=1, sequence_id=2,
                      task_name="Load Employees", source_type="DBX_NOTEBOOK",
                      source_system_code="LoadEmployees", task_mandatory=True,
                      expected_duration_seconds=300)

# Register watermarks (once)
monitor.register_parameter("CORP", "HR_DAILY", "SYSDT", "SYSTEM")
monitor.register_parameter("CORP", "HR_DAILY", "LoadEmployees", "DELTA_DATE",
                           description="Last loaded employee timestamp")

# Each run
exec_id = ETLMonitorFramework.generate_execution_id()
monitor.generate_execution_steps(exec_id, "CORP", "HR_DAILY", "2026-04-09")

with monitor.task(exec_id, "CORP", "HR_DAILY",
                  task_id=1, workflow_id=1, sequence_id=2,
                  processing_date="2026-04-09"):
    pass   # your notebook logic here
```

---

## Sample Notebooks

After installing, extract the sample notebooks to your workspace:

```python
SAMPLE_USAGE_PATH = monitor.sample_usage(spark)
```

This copies four notebooks to `/Workspace/Users/{you}/databricks-etl-monitor/sample_usage/`:

| Notebook | Purpose |
|----------|---------|
| `00-infrastructure.py` | Create catalog and ETL schema (run once per environment) |
| `01-install.py` | Install framework, call `setup()`, extract samples |
| `02-config.py` | Register a process, tasks, and watermark parameters |
| `03-run.py` | Full execution run with status queries and retry demo |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│           MONITORING VIEWS  (catalog.etl schema)                      │
│  v_processStatus   — cross-process live dashboard                     │
│  v_runSummary      — per execution/attempt rollup                     │
│  v_taskDetail      — task-level with SLA breach flag                  │
│  v_mandatoryBlockers — tasks blocking downstream progress             │
│  v_currentFailures — latest-attempt failures                          │
│  v_watermarks      — watermark state + ActiveValue (ADF Lookup)       │
└──────────────────────┬───────────────────────────────────────────────┘
                        │ reads from
┌──────────────────────▼───────────────────────────────────────────────┐
│           TRACKING TABLES  (catalog.etl schema)                       │
│  ETLconfigSequence   [FRAMEWORK-MANAGED — 7 built-in stages]          │
│  ETLconfigProcess    [USER-MANAGED — domain process registry]         │
│  ETLconfigTasks      [USER-MANAGED — task catalogue]                  │
│  ETLconfigParameters [USER-MANAGED — watermarks + config flags]       │
│  ETLProcessingSteps  [RESULTS — per-task live log, mutable]           │
│  ETLsysLogs          [RESULTS — raw run receipts, append-only]        │
└──────────────────────┬───────────────────────────────────────────────┘
                        │ Python SDK / Spark SQL / JDBC
┌──────────────────────────────────────────────────────────────────────┐
│           CONSUMERS                                                    │
│  DBX Notebooks  → monitor.task() context manager                      │
│  DBX Jobs       → start_task() / end_task() / fail_task()            │
│  ADF Pipelines  → Databricks Notebook activity (utility notebooks)    │
│                   + Lookup activity reads v_watermarks.ActiveValue    │
│  SQL Server     → JDBC linked server reads Delta views                │
│  Dataflows      → post-activity webhook or notebook shim              │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Tables Created by `setup()`

| Table | Managed by | Purpose |
|-------|-----------|---------|
| `ETLconfigSequence` | Framework | 7 workflow stage definitions (auto-seeded) |
| `ETLconfigProcess` | User | Domain process registry — one row per domain load |
| `ETLconfigTasks` | User | Task catalogue — what runs, in what order, how often |
| `ETLconfigParameters` | User | Delta watermarks and config flags per process |
| `ETLProcessingSteps` | Results | Per-task live execution log (mutable, partitioned by date) |
| `ETLsysLogs` | Results | Raw ADF/DBX run receipts (append-only, IDENTITY PK) |

---

## Workflow Stage Definitions (`ETLconfigSequence`)

Seeded automatically by `setup()`.  All tasks sharing a `SequenceID` run in **parallel**.

| SequenceID | SequenceCode | Description |
|-----------|-------------|-------------|
| 0 | `LOAD_GO` | Initiating ETL Processing — overall run marker |
| 1 | `LOAD_DB_CONFIG` | Load Configuration Data from source |
| 2 | `LOAD_DB_TRAN` | Load Transactional Data from source (staging) |
| 3 | `LOAD_DIM` | Process Master Data — validate staged dimensions |
| 4 | `LOAD_TRAN` | Process Transactional Data — validate staged transactions |
| 5 | `PRE_PROCESS` | Functional Logic — business logic and derivations |
| 6 | `PROCESS_DATA` | Core Data Transformation — output / data mart tables |

Custom stages: use `SequenceID >= 10` to avoid collision with framework rows.

---

## WorkFlowID Semantics

| WorkFlowID | Meaning |
|-----------|---------|
| 0 | Initiation task — always `TaskID=0`, `SequenceID=0`; one per process; overall run status indicator |
| 1 | First workflow pass (main load) |
| 2 | Second pass (enrichment / additional fields / second data iteration) |
| N | Nth iteration over the same data with a different scope |

---

## Reporting Views

| View | Purpose |
|------|---------|
| `v_processStatus` | Cross-process live dashboard by processing date |
| `v_runSummary` | Run-level rollup with task counts per status |
| `v_taskDetail` | Per-task detail with SLA breach flag, filtered by ExecutionID |
| `v_mandatoryBlockers` | Mandatory failed tasks preventing downstream progress |
| `v_currentFailures` | All failed tasks for current / specified date |
| `v_watermarks` | Current watermark values with resolved `ActiveValue` (ADF Lookup bridge) |

---

## Status Values

```
NQUE  (New Queue)   — task created, first attempt, awaiting execution
RQUE  (Re-Queue)    — reset from FAIL, retry attempt queued
DONE                — completed successfully
FAIL                — failed — awaiting retry or investigation
```

State machine:
```
NQUE → DONE
NQUE → FAIL → RQUE → DONE
NQUE → FAIL → RQUE → FAIL → [manual status_reset()] → RQUE → DONE
```

---

## ParameterType Values

| Type | Active column | Auto-advance on DONE? | Bulk mode |
|------|--------------|----------------------|-----------|
| `DELTA_DATE` | `ValueDateTime` | Yes — set to task `StartTime` | `ValueDateTime = NULL` |
| `DELTA_ID` | `ValueINT` | No — call `advance_watermark()` | `ValueINT = 0` |
| `FLAG` | `ValueBIT` | No — read freely | Not applicable |
| `SYSTEM` | `ValueDateTime` | No — `set_processing_mode()` only | `NULL` = live date |

**KNOWN LIMITATION — DELTA_ID:** For ID-based watermarks the framework cannot auto-detect
the max ID from the source dataset.  Developer must call `advance_watermark()` explicitly
after their load logic completes:

```python
max_id = df.agg({"EmployeeID": "max"}).collect()[0][0]
monitor.advance_watermark("CORP", "HR_DAILY", "LoadEmployeesByID", new_int_value=max_id)
```

---

## ADF Integration

### Watermark lookup

ADF Lookup activity reads `ActiveValue` — a resolved STRING regardless of ParameterType:

```json
{
  "type": "Lookup",
  "name": "GetWatermark",
  "source": {
    "query": "SELECT ActiveValue FROM `<catalog>`.`etl`.`v_watermarks` WHERE ProjectCode='CORP' AND ProcessLoad='HR_DAILY' AND ParameterName='LoadEmployees'"
  }
}
```

ADF Copy Activity source query expression:
```
@concat('SELECT * FROM dbo.Employees WHERE ModifiedDate > ''',
        activity('GetWatermark').output.firstRow.ActiveValue, '''')
```

### Write-back via utility notebooks

ADF calls a Databricks Notebook activity passing widget parameters.
Three lightweight utility notebooks are created per project (not shipped with this package):

- `etl_start_task.py` — receives `execution_id`, `project_code`, `process_load`, `task_id`,
  `workflow_id`, `sequence_id`, `processing_date`, `source_type` as widgets → calls `monitor.start_task(...)`.
- `etl_end_task.py` — same widgets + `log_message`, `log_type` → calls `monitor.end_task(...)`.
- `etl_fail_task.py` — same widgets + error details → calls `monitor.fail_task(...)`.

### ForEach over pending tasks

ADF ForEach iterates `get_pending_tasks()` output.
Tasks sharing the same `SequenceID` are dispatched in parallel (ADF parallel ForEach).
After each SequenceID stage completes, ADF checks `v_mandatoryBlockers` before advancing.

---

## Stored Procedure Equivalence

For teams migrating from the SQL Server / ADF version:

| Original stored procedure | Python method | Notes |
|--------------------------|--------------|-------|
| `p_ETLProcessingSteps` (GenerateMode=1) | `generate_execution_steps()` | INSERT NQUE rows for all active tasks |
| `p_ETLOrchestrationSteps` | `get_pending_tasks()` | Returns non-DONE tasks; auto-generates on first call |
| `p_ETLProcessingStatusUpdate` | `end_task()` / `fail_task()` | Status + timing write-back; `DELTA_DATE` auto-advance on DONE |
| `p_ETLProcessingStatusGet` | `get_status()` | Summary or task-level detail; `summary_mode=True` for rollup |
| `p_ETLProcessingStatusReset` | `status_reset()` | Bulk or specific task reset; always resets initiation row |
| `p_ETLconfigProcessingMode` | `set_processing_mode()` | Historic mode, live mode, bulk mode, specific param |

---

## Processing Mode

```python
monitor.set_processing_mode("CORP", "HR_DAILY", is_bulk_mode=True)          # full reload
monitor.set_processing_mode("CORP", "HR_DAILY", is_historic_mode=True,
                             historic_date="2026-01-01")                     # historic rerun
monitor.set_processing_mode("CORP", "HR_DAILY")                             # restore live mode
```

---

## Status and Retry

```python
# Check status
monitor.get_status("CORP", "HR_DAILY", execution_id=exec_id)           # task detail
monitor.get_status("CORP", "HR_DAILY", summary_mode=True)              # run rollup

# Reset failures for retry
monitor.status_reset("CORP", "HR_DAILY", execution_id=exec_id)         # all failures in run
monitor.status_reset("CORP", "HR_DAILY", execution_id=exec_id,
                     task_id=1, workflow_id=1)                          # specific task only
```

---

## Recommended Implementation Order

1. **Deploy** — run `monitor.setup()` in a Databricks notebook (idempotent, creates everything).
2. **Register processes** — `register_process()` for each domain (HR_DAILY, FIN_MONTHLY, etc.).
3. **Register tasks** — `register_task()` with `TaskID`, `WorkFlowID`, `SequenceID` per task.
4. **Register parameters** — `register_parameter()` with `ParameterType` for each watermark.
5. **Instrument** — add `monitor.task()` context manager to 2–3 pilot notebooks.
6. **Verify** — query `v_taskDetail` and `v_runSummary` after a pilot run.
7. **Dashboard** — Lakeview dashboard reading from the 6 monitoring views.
8. **ADF integration** — utility notebooks for ADF write-back; Lookup activity for watermarks.

---

## What Was NOT Ported

| Original component | Reason not ported |
|--------------------|------------------|
| Trigger / orchestration logic | Out of scope — this framework observes only, never triggers |
| `#DELTAPARAMETER#` string substitution | Replaced by `v_watermarks.ActiveValue` |
| `ADFMain` / `ADFPipelines` / `ADFMetaData` | ADF pipeline driver config — not needed for monitoring |
| `ETLconfigNotifications` | Replace with Databricks SQL Alerts or Lakeview dashboards |
| T-SQL stored procedures | Replaced entirely by Python class methods |

---

## License

MIT
