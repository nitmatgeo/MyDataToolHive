# Databricks ETL Monitor Framework

A metadata-driven ETL process monitoring framework for Databricks / Delta Lake.

Tracks ADF pipelines, Databricks notebooks, Databricks jobs, and Dataflows in a single
Unity Catalog schema — without modifying how those jobs are triggered.

## Quick Start

```python
%pip install databricks-etl-monitor --upgrade --no-deps

from etl_monitor import ETLMonitorFramework

monitor = ETLMonitorFramework(spark, catalog="main", schema="etl")
monitor.setup()   # idempotent — creates 6 tables, 6 views, seeds sequence stages

# Register a domain process (once)
monitor.register_process("RETAIL", "DAILY_LOAD", name="Retail Daily Load", load_frequency="D")

# Register tasks (once)
monitor.register_task("RETAIL", "DAILY_LOAD", task_id=0, workflow_id=0, sequence_id=0, task_name="Initiation")
monitor.register_task("RETAIL", "DAILY_LOAD", task_id=1, workflow_id=1, sequence_id=2,
                      task_name="Load Products", source_system_code="LoadProducts")

# Register watermarks (once)
monitor.register_parameter("RETAIL", "DAILY_LOAD", "SYSDT", "SYSTEM")
monitor.register_parameter("RETAIL", "DAILY_LOAD", "LoadProducts", "DELTA_DATE")

# Each run
exec_id = ETLMonitorFramework.generate_execution_id()
monitor.generate_execution_steps(exec_id, "RETAIL", "DAILY_LOAD", "2026-04-09")

with monitor.task(exec_id, "RETAIL", "DAILY_LOAD",
                  task_id=1, workflow_id=1, sequence_id=2,
                  processing_date="2026-04-09"):
    pass   # your notebook logic here
```

## Sample Notebooks

After installing, extract the sample notebooks to your workspace:

```python
monitor.sample_usage(spark)
```

This copies four notebooks to `/Workspace/Users/{you}/databricks-etl-monitor/sample_usage/`:

| Notebook | Purpose |
|----------|---------|
| `00-infrastructure.py` | Create catalog and ETL schema (run once per environment) |
| `01-install.py` | Install framework, call `setup()`, extract samples |
| `02-config.py` | Register a process, tasks, and watermark parameters |
| `03-run.py` | Full execution run with status queries and retry demo |

## Tables Created by `setup()`

| Table | Managed by | Purpose |
|-------|-----------|---------|
| `ETLconfigSequence` | Framework | 7 workflow stage definitions (LOAD_GO → PROCESS_DATA) |
| `ETLconfigProcess` | User | Domain process registry |
| `ETLconfigTasks` | User | Task catalogue per process |
| `ETLconfigParameters` | User | Delta watermarks and config flags |
| `ETLProcessingSteps` | Results | Per-task live execution log (mutable) |
| `ETLsysLogs` | Results | Raw run receipts (append-only) |

## Reporting Views

| View | Purpose |
|------|---------|
| `v_processStatus` | Cross-process live dashboard by processing date |
| `v_runSummary` | Run-level rollup with task counts per status |
| `v_taskDetail` | Per-task detail filtered by ExecutionID |
| `v_mandatoryBlockers` | Mandatory failed tasks preventing downstream progress |
| `v_currentFailures` | All failed tasks for current/specified date |
| `v_watermarks` | Current watermark values with resolved `ActiveValue` (ADF bridge) |

## ADF Integration

ADF Lookup activity reads `ActiveValue` — a resolved STRING regardless of ParameterType:

```sql
SELECT ActiveValue FROM `<catalog>`.`etl`.`v_watermarks`
WHERE ProjectCode='RETAIL' AND ProcessLoad='DAILY_LOAD' AND ParameterName='LoadProducts'
```

## ParameterType Values

| Type | Behaviour |
|------|-----------|
| `DELTA_DATE` | `ValueDateTime` watermark — auto-advanced to task `StartTime` on DONE |
| `DELTA_ID` | `ValueINT` watermark — call `advance_watermark()` manually after load |
| `FLAG` | `ValueBIT` boolean config — not auto-advanced |
| `SYSTEM` | Reserved for `SYSDT` — controlled via `set_processing_mode()` only |

## Status Values

`NQUE` (queued) → `DONE` (success) | `FAIL` (failure) → `RQUE` (retry queued) → `DONE`

## License

MIT
