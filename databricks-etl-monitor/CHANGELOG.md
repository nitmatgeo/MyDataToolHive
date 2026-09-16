# Changelog

All notable changes to `databricks-etl-monitor` are documented here.

---

## [1.0.0] — 2026-09-16

First stable release.

- Metadata-driven ETL task orchestration via Delta tables — no pipeline code changes needed
- Four-level hierarchy: Organisation → Project → Process → Task
- Task lifecycle management: NQUE → IN PROGRESS → DONE / FAIL via context manager
- Control flags: `ForceSkip`, `IsActive`, `TaskMandatory`, `SequenceID` — updated in config tables, read at runtime
- Idempotent retry: `generate_execution_steps()` re-queues only non-DONE tasks
- Processing modes: bulk (reset all watermarks), historic (replay a past date), live
- Six auto-created SQL views: `v_processStatus`, `v_runSummary`, `v_taskDetail`, `v_mandatoryBlockers`, `v_currentFailures`, `v_watermarks`
- `guide()` and `sample_usage(spark)` helpers for onboarding
- PyPI trusted publishing (OIDC) via GitHub Actions

## [0.1.2] — initial pre-release iterations

Early development and internal testing releases.
