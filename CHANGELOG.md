# Changelog

All notable changes to `databricks-etl-monitor` are documented here.

---

## [0.1.0] — 2026-04-19

### Added
- Initial open-source release of the Databricks ETL Monitor Framework.
- `ETLMonitorFramework` class — main entry point.
- `setup()` — idempotent schema + table + view + seed creation.
- Six Delta tables: `ETLconfigSequence`, `ETLconfigProcess`, `ETLconfigTasks`,
  `ETLconfigParameters`, `ETLProcessingSteps`, `ETLsysLogs`.
- Six reporting views: `v_processStatus`, `v_runSummary`, `v_taskDetail`,
  `v_mandatoryBlockers`, `v_currentFailures`, `v_watermarks`.
- `register_organisation()`, `register_project()`, `register_process()`, `register_task()`,
  `register_parameter()` — INSERT-ONLY MERGE config writers.
- `generate_execution_steps()` — Attempts-aware NQUE row generation; period-aware NOT EXISTS
  for M/Y frequency tasks; `FullFileName` computed by `LoadFrequency` at generate time.
- `get_pending_tasks()` — two modes mirroring original `p_ETLProcessingSteps`:
  - Orchestration (no `task_id`): non-DONE + `IsActive=TRUE` + `ForceSkip=FALSE` tasks.
  - Per-task (`task_id` supplied): always 1 row; `Status='NULL'` = deactivated / ForceSkip / DONE / not generated.
- `task()` — self-guarding context manager; yields `_TaskGuard(active, status)`. Checks status
  at entry — skips silently (`t.active=False`) when `IsActive=FALSE`, `ForceSkip=TRUE`, already
  DONE, or not generated. No start/end DB writes on skip.
- `task_status()` — returns `'NQUE'`/`'RQUE'`/`'FAIL'`/`'NULL'` for explicit pre-checks.
- `skip_task()` — sets `ForceSkip=TRUE` for a specific task in a specific run only.
- `unskip_task()` — clears `ForceSkip` back to `FALSE` within the same execution.
- `start_task()`, `end_task()`, `fail_task()` — low-level task state transitions; `attempts`
  auto-detected from NQUE/RQUE row; optional `timestamp` override for ADF utility notebooks.
- `advance_watermark()` — manual DELTA_ID watermark advance (KNOWN LIMITATION).
- `get_active_watermark()` — reads typed watermark value (str / int / bool).
- `get_status()` — task detail or summary rollup DataFrame.
- `status_reset()` — resets DONE → RQUE for day replay; also clears `ForceSkip=FALSE` on all
  rows in scope. Not for failure retry — use a new ExecutionID for that.
- `set_processing_mode()` — bulk / historic / live mode control via SYSDT.
- `generate_execution_id()` — static UUID generator.
- `sample_usage()` — extracts bundled sample notebooks to `/Workspace/Users/{user}/`.
- Four sample notebooks: `00-infrastructure`, `01-install`, `02-config`, `03-run`.
- `ParameterType` enum: `DELTA_DATE` (auto-advance on DONE), `DELTA_ID` (manual), `FLAG`, `SYSTEM`.
- Seven built-in sequence stages seeded into `ETLconfigSequence` (LOAD_GO → PROCESS_DATA).
- `v_watermarks.ActiveValue` STRING bridge column for ADF Lookup activity integration.
- `ForceSkip BOOLEAN` on `ETLProcessingSteps` — run-level task exclusion, `FALSE` by default.
- `_TaskGuard` — guard object with `active` (bool) and `status` (str) fields.

### ForceSkip vs IsActive

| | `ETLconfigTasks.IsActive` | `ETLProcessingSteps.ForceSkip` |
|---|---|---|
| Scope | Permanent — all future runs | One run only (this ExecutionID) |
| Carries to retry? | Yes — config persists | No — new NQUE row = FALSE |
| Cleared by `status_reset()`? | No | Yes — replay = clean slate |
| Use case | Task retired / under maintenance | Upstream dep not ready for this run |
