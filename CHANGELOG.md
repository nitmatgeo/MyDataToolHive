# Changelog

## [0.1.0a1] — 2026-04-19

### Added
- Initial pre-release alpha.
- `ExcelIngestFramework` — main class with full 4-stage pipeline.
- Stage 1: `validation.py` — file existence, format, password detection, sheet listing.
- Stage 2: `structure.py` — auto-detection of merged cells, multi-row headers, hidden/blank columns.
- Stage 3: `metadata.py` — hierarchical header reconstruction (`[Parent].[Child]`), SHA-256 signature, section detection.
- Stage 4: `mapping/engine.py` — hybrid confidence mapping (70 % rule-based + 30 % LLM).
- Pluggable LLM adapters: Databricks Foundation Models, OpenAI, Anthropic (all optional extras).
- `utils/paths.py` — `FileLocationType` enum + `detect_location_type()` supporting Unity Catalog Volumes, DBFS, Azure Storage, local paths.
- Numbered `sample_usage/` notebooks (01–05).
