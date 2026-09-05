# Databricks ETL Monitor Framework

A metadata-driven ETL process monitoring framework for Databricks / Delta Lake.

Tracks ADF pipelines, Databricks notebooks, Databricks jobs, and Dataflows in a single
Unity Catalog schema — **without modifying how those jobs are triggered**.

Ported from a mature SQL Server / ADF ETL orchestration framework.  The monitoring and
tracking patterns have been reimplemented as Unity Catalog Delta tables and a self-creating
Python class, making them available to pure Databricks teams without a SQL Server dependency.

---

> [!NOTE]
> **Get started in 2 minutes**
>
> ```bash
> pip install databricks-etl-monitor
> ```
>
> In a Databricks notebook:
>
> ```python
> %pip install databricks-etl-monitor
> dbutils.library.restartPython()
> ```
>
> Then extract the sample notebooks directly into your workspace — they walk through full setup, config, and a live run:
>
> ```python
> from etl_monitor import ETLMonitorFramework
> monitor = ETLMonitorFramework(spark, catalog="<your_catalog>", schema="etl")
> monitor.sample_usage(spark)   # extracts 00-infrastructure through 03-run into your Workspace
> ```
>
> Run `00-infrastructure.py` first to create the catalog and ETL schema, then `01-install.py` to install the framework on the cluster and call `setup()` — which creates all 6 Delta tables, 6 views, and seeds the workflow stage definitions. Both are idempotent and safe to re-run.

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
                      source_system_code="LoadEmployees", task_mandatory=True)

# Register watermarks (once)
monitor.register_parameter("CORP", "HR_DAILY", "SYSDT", "SYSTEM")
monitor.register_parameter("CORP", "HR_DAILY", "LoadEmployees", "DELTA_DATE",
                           description="Last loaded employee timestamp")

# Each run
exec_id = ETLMonitorFramework.generate_execution_id()
monitor.generate_execution_steps(exec_id, "CORP", "HR_DAILY", "2026-04-09")

with monitor.task(exec_id, "CORP", "HR_DAILY",
                  task_id=1, workflow_id=1, sequence_id=2,
                  processing_date="2026-04-09") as t:
    if t.active:
        pass   # your notebook logic here
    # t.active=False when task is deactivated, force-skipped, already DONE, or not generated
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

All three share the same widget set:
`execution_id`, `project_code`, `process_load`, `task_id`, `workflow_id`, `sequence_id`,
`processing_date`, `source_type`, `log_message`, `log_type`, `log_code`, `timestamp`

- `etl_start_task.py` → calls `monitor.start_task(...)`. `timestamp` overrides the recorded start time (pass ADF activity start time when it differs from notebook execution time).
- `etl_end_task.py` → calls `monitor.end_task(...)`. `DurationSeconds` computed from `StartTime` to `timestamp`.
- `etl_fail_task.py` → calls `monitor.fail_task(...)`. `log_message` carries the ADF error output.

### ForEach over pending tasks

ADF ForEach iterates `get_pending_tasks()` output.
Tasks sharing the same `SequenceID` are dispatched in parallel (ADF parallel ForEach).
After each SequenceID stage completes, ADF checks `v_mandatoryBlockers` before advancing.

---

## API Reference

| Method | Notes |
|--------|-------|
| `generate_execution_steps()` | INSERT NQUE rows; Attempts-aware; period-aware skip for M/Y tasks |
| `get_pending_tasks(..., task_id=N, workflow_id=N, sequence_id=N)` | Always 1 row; `Status='NULL'` = skip |
| `get_pending_tasks()` (no `task_id`) | Non-DONE + `IsActive=TRUE` + `ForceSkip=FALSE` tasks; auto-generates on first call |
| `end_task()` / `fail_task()` | Status + timing write-back; `DELTA_DATE` auto-advance on DONE; `Attempts` auto-detected |
| `get_status()` | Summary or task-level detail; `summary_mode=True` for rollup |
| `status_reset()` | Day replay only (DONE → RQUE); also clears `ForceSkip`. **Not** for failure retry — use a new ExecutionID |
| `set_processing_mode()` | Historic mode, live mode, bulk mode, specific param |
| `task_status()` | Returns `'NQUE'`/`'RQUE'`/`'FAIL'`/`'NULL'` — explicit pre-check without entering `task()` |
| `skip_task()` / `unskip_task()` | Run-level `ForceSkip` flag — excludes one task from one run without touching config |

---

## Processing Mode

```python
monitor.set_processing_mode("CORP", "HR_DAILY", is_bulk_mode=True)          # full reload
monitor.set_processing_mode("CORP", "HR_DAILY", is_historic_mode=True,
                             processing_date="2026-01-01")                   # historic rerun
monitor.set_processing_mode("CORP", "HR_DAILY")                             # restore live mode
```

---

## Status, Retry and Day Replay

```python
# Check status
monitor.get_status("CORP", "HR_DAILY", execution_id=exec_id)           # task detail
monitor.get_status("CORP", "HR_DAILY", summary_mode=True)              # run rollup

# Failure retry — use a NEW ExecutionID (mirrors ADF re-trigger with a new RunID)
# generate_execution_steps() auto-increments Attempts, skips DONE tasks, re-inserts FAIL/NQUE only
exec_id_2 = ETLMonitorFramework.generate_execution_id()
monitor.generate_execution_steps(exec_id_2, "CORP", "HR_DAILY", "2026-04-09")

# Day replay — reset an already-completed date back to RQUE (NOT for failure retry)
# Also clears any ForceSkip flags so all tasks run cleanly on replay
monitor.status_reset("CORP", "HR_DAILY", processing_date="2026-04-09")  # full date
monitor.status_reset("CORP", "HR_DAILY", execution_id=exec_id,
                     task_id=1, workflow_id=1)                           # specific task
```

---

## Task Exclusion — IsActive vs ForceSkip

Two mechanisms for skipping tasks. Use the right one for the scope of the decision.

| | `ETLconfigTasks.IsActive` | `ETLProcessingSteps.ForceSkip` |
|---|---|---|
| Scope | **Permanent** — all future runs | **One run only** — this ExecutionID |
| Carries to retry? | Yes — config persists | No — new NQUE row = `FALSE` |
| Cleared by `status_reset()`? | No | Yes — day replay = clean slate |
| Use case | Task retired / under maintenance | Upstream dep not ready for this run only |

```python
# Skip one task for this run only (config unchanged — IsActive stays TRUE)
monitor.skip_task(exec_id, "CORP", "HR_DAILY",
                  task_id=4, workflow_id=1, sequence_id=2, processing_date="2026-04-09")

# task() picks it up automatically — same code, no branching needed by caller
with monitor.task(exec_id, "CORP", "HR_DAILY",
                  task_id=4, workflow_id=1, sequence_id=2,
                  processing_date="2026-04-09") as t:
    if t.active:
        actual_work()    # only runs when NQUE/RQUE/FAIL and not skipped
    else:
        print(f"→ skipped ({t.status})")

# Re-enable mid-run (dependency resolved)
monitor.unskip_task(exec_id, "CORP", "HR_DAILY",
                    task_id=4, workflow_id=1, sequence_id=2, processing_date="2026-04-09")

# Explicit status check without entering the with block
status = monitor.task_status(exec_id, "CORP", "HR_DAILY", "2026-04-09",
                              workflow_id=1, sequence_id=2, task_id=4)
# Returns 'NQUE', 'RQUE', 'FAIL', or 'NULL'
```

---

## Implementation Guide

### One-time setup (per environment)

Run these once when deploying the framework to a new catalog / environment.
All steps are idempotent — safe to re-run without data loss (see note below).

1. **Deploy** — `monitor.setup()` creates the schema, 6 tables, 6 views, and seeds sequence stages.
2. **Register organisation / project** — `register_organisation()`, `register_project()` once per entity.
3. **Register processes** — `register_process()` once per domain (e.g. `HR_DAILY`, `FIN_MONTHLY`).
4. **Register tasks** — `register_task()` with `TaskID`, `WorkFlowID`, `SequenceID` per task.
5. **Register watermarks** — `register_parameter()` with `ParameterType` per watermark or flag.
6. **Instrument notebooks** — add `monitor.task()` context manager to each notebook / job.
7. **ADF integration** — create utility notebooks (`etl_start_task`, `etl_end_task`, `etl_fail_task`) per project; wire ADF Lookup activity to `v_watermarks.ActiveValue`.
8. **Dashboard** — Lakeview dashboard reading from the 6 monitoring views.

> **What `setup()` does on a re-run:**
> - `CREATE TABLE IF NOT EXISTS` — skips if table exists; **existing data is never touched**.
> - `CREATE OR REPLACE VIEW` — recreates all 6 views (views hold no data, always safe).
> - `seed_sequence_data()` — INSERT-ONLY MERGE on `ETLconfigSequence`; existing rows unchanged.
> - `ALTER TABLE ETLProcessingSteps ADD COLUMN ...` — adds any new columns (e.g. `ForceSkip`) to existing tables; no-op if already present.
> - Config tables (`ETLconfigTasks`, `ETLconfigParameters`, etc.) and results tables (`ETLProcessingSteps`, `ETLsysLogs`) are **never modified or truncated** by `setup()`.

### Adding new config (ongoing)

Run these whenever the process catalogue grows — no re-deployment needed.

- **New process / domain** — `register_process()` then `register_task()` / `register_parameter()`.
- **New task in existing process** — `register_task()` with the new `TaskID`. Next `generate_execution_steps()` call picks it up automatically.
- **New watermark** — `register_parameter()`. Takes effect from the next run.
- **Deactivate a task permanently** — update `IsActive=FALSE` in `ETLconfigTasks` (or call `register_task(is_active=False)`). No steps generated for it going forward.

### Every run (automated)

These are called by instrumented notebooks, jobs, or ADF activities on each execution.

1. `generate_execution_steps(exec_id, project, process, date)` — creates NQUE rows for all active tasks.
2. `monitor.task(...) as t` — in each notebook/job cell: starts, tracks, and closes the task automatically.
3. `get_pending_tasks(...)` — ADF ForEach source; returns runnable tasks with watermarks and file paths.
4. `advance_watermark(...)` — after DELTA_ID loads only (DELTA_DATE is auto-advanced on DONE).

---

## Future Plans

### 1. Native orchestrator

Add an `orchestrate()` method that drives task execution order natively within Databricks —
iterating over `get_pending_tasks()` by `WorkFlowID` / `SequenceID`, dispatching parallel tasks
within each stage, and checking `v_mandatoryBlockers` before advancing to the next stage.
This would remove the need for ADF ForEach or a separate Databricks Workflow definition
for teams who want a pure-Python orchestration path.

### 2. ETL notifications (`ETLconfigNotifications`)

A notifications config table and a `notify()` method that sends execution status emails natively
from within Databricks, scoped to a specific process, run, or task range:

- **Per-process recipient config** — `to`, `cc`, `bcc` addresses registered per `ProjectCode / ProcessLoad`; optional filters by `WorkFlowID`, `SequenceID`, `TaskID`.
- **Trigger conditions** — on FAIL, on DONE, on mandatory blocker, or on completion of a specific `SequenceID` stage.
- **HTML email body** — auto-generated from the execution log for the specified run scope: status summary table, per-task rows (WID / SID / TID / TaskName / Status / Duration / LogMessage), watermark values, and a run health indicator.
- **Native Databricks delivery** — via SMTP or Databricks SQL Alerts webhook, no external orchestrator needed.

---

## License

MIT
