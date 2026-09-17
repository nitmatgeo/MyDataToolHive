# Changelog

All notable changes to `databricks-excel-ingest-framework` are documented here.

---

## [1.0.0] — 2026-09-16

First stable release.

- Structured Excel-to-Delta ingestion pipeline: validate → detect → extract → map (four stages via `ingest()`), followed by an explicit `load()` call
- Handles heterogeneous Excel inputs: multiple sheets, dynamic headers, merged cells, varying column names across file versions
- Canonical field mapping: caller-supplied alias dictionary resolved automatically; optional LLM-assisted fuzzy matching for unknown column names
- Adapter pattern for target systems: `DatabricksAdapter` for Delta writes, extensible to other targets
- Result object with `summary_record()` for audit logging
- Formulated Excel configuration template with dropdowns generating PySpark API calls and SQL `UNION ALL SELECT` statements per config row
- PyPI trusted publishing (OIDC) via GitHub Actions

## [0.1.2] — initial pre-release iterations

Early development and internal testing releases.
