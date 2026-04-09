# Changelog

All notable changes to `databricks-etl-monitor` are documented here.

---

## [0.1.0] — 2026-04-09

### Added
- Initial open-source release of the Databricks ETL Monitor Framework.
- `ETLMonitorFramework` class — main entry point.
- `setup()` — idempotent schema + table + view + seed creation.
- Six Delta tables: `ETLconfigSequence`, `ETLconfigProcess`, `ETLconfigTasks`,
  `ETLconfigParameters`, `ETLProcessingSteps`, `ETLsysLogs`.
- Six reporting views: `v_processStatus`, `v_runSummary`, `v_taskDetail`,
  `v_mandatoryBlockers`, `v_currentFailures`, `v_watermarks`.
- `register_process()`, `register_task()`, `register_parameter()` — INSERT-ONLY MERGE config writers.
- `generate_execution_steps()` — creates NQUE rows for all active tasks in a run.
- `get_pending_tasks()` — returns non-DONE tasks; auto-generates steps on first call.
- `task()` — context manager: NQUE → DONE on success, NQUE → FAIL on exception.
- `start_task()`, `end_task()`, `fail_task()` — low-level task state transitions.
- `advance_watermark()` — manual DELTA_ID watermark advance.
- `get_active_watermark()` — reads typed watermark value (str / int / bool).
- `get_status()` — task detail or summary rollup DataFrame.
- `status_reset()` — resets FAIL → RQUE for retry (whole run or specific task).
- `set_processing_mode()` — bulk / historic / live mode control via SYSDT.
- `generate_execution_id()` — static UUID generator.
- `sample_usage()` — extracts bundled sample notebooks to `/Workspace/Users/{user}/`.
- Four sample notebooks: `00-infrastructure`, `01-install`, `02-config`, `03-run`.
- `ParameterType` enum: `DELTA_DATE` (auto-advance), `DELTA_ID` (manual), `FLAG`, `SYSTEM`.
- Seven built-in sequence stages seeded into `ETLconfigSequence` (LOAD_GO → PROCESS_DATA).
- `v_watermarks.ActiveValue` STRING bridge column for ADF Lookup activity integration.
