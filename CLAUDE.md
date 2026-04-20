# databricks-excel-ingest-framework — Subproject Context

## What this folder is

A PyPI-publishable Python package (`databricks-excel-ingest-framework`, import: `excel_ingest`)
for Excel file ingestion on Databricks. Supersedes `Excel-Ingestion-DBXFramework/` — that folder
will be deleted once this is complete. Version: `0.1.0a11` (pre-release alpha 11).

Primary target is Databricks (Unity Catalog Volumes, DBFS, Foundation Models). Core modules
(validation, structure, metadata) are platform-independent — openpyxl only, no Databricks
dependency. LLM mapping adapters are optional extras. Version: `0.1.0a14` (pre-release alpha 12, last published: a10).

---

## Files in this folder

| Path | Purpose |
|------|---------|
| `pyproject.toml` | Package metadata, dependencies, optional extras |
| `CLAUDE.md` | This file — subproject context for Claude |
| `README.md` | User-facing overview and quick start |
| `CHANGELOG.md` | Version history |
| `build_and_publish.bat` | Build wheel and publish to PyPI |
| `.gitignore` | Excludes dist/, __pycache__, *.egg-info |
| `excel_ingest/__init__.py` | Package exports + version |
| `excel_ingest/framework.py` | `ExcelIngestFramework` — main orchestration class |
| `excel_ingest/validation.py` | Stage 1 — file validation (existence, format, password, sheets) |
| `excel_ingest/structure.py` | Stage 2 — header/structure detection (merged cells, multi-row) |
| `excel_ingest/metadata.py` | Stage 3 — hierarchical metadata extraction + SHA-256 signature |
| `excel_ingest/mapping/__init__.py` | Mapping submodule exports |
| `excel_ingest/mapping/engine.py` | Stage 4 — mapping orchestration (rule + optional LLM) |
| `excel_ingest/mapping/confidence.py` | Confidence scoring, thresholds, MappingStatus enum |
| `excel_ingest/mapping/adapters/__init__.py` | Adapter exports |
| `excel_ingest/mapping/adapters/base.py` | Abstract `LLMAdapter` + `LLMResponse` dataclass |
| `excel_ingest/mapping/adapters/databricks.py` | Databricks Foundation Models adapter (parameterised model) |
| `excel_ingest/mapping/adapters/openai.py` | OpenAI Chat Completions adapter |
| `excel_ingest/mapping/adapters/anthropic.py` | Anthropic Messages adapter |
| `excel_ingest/utils/__init__.py` | Utils exports |
| `excel_ingest/utils/paths.py` | `FileLocationType` enum + `detect_location_type()` |
| `excel_ingest/sample_usage/` | Numbered sample notebooks (01–05) |

---

## Architecture

```
ExcelIngestFramework(spark, catalog, schema, adapter)
├── .validate(file_path, password)               → FileValidationResult
├── .detect_structure(file_path, config, password) → FileStructureMetadata
├── .extract_metadata(file_path, structure, file_id, password) → MetadataExtractionResult
├── .map_to_canonical(metadata, canonical_dict, country_code)  → List[CanonicalMapping]
├── .ingest(file_path, canonical_dict, ...)       → IngestResult  [full pipeline]
├── .guide()                                      → None  [step-by-step usage printed to stdout]
└── .sample_usage(spark)                          → str   [extracts bundled notebooks to Workspace]

LLM adapters (all optional):
├── DatabricksAdapter(model="databricks-llama-3-70b-instruct", host=None)
├── OpenAIAdapter(model="gpt-4o-mini", api_key=None)
└── AnthropicAdapter(model="claude-haiku-4-5-20251001", api_key=None)
```

---

## Naming conventions

| Aspect | Convention |
|--------|-----------|
| PyPI package | `databricks-excel-ingest-framework` |
| Python import | `excel_ingest` |
| Main class | `ExcelIngestFramework` |
| Module files | `snake_case.py` |
| Dataclasses | `PascalCase` |
| Enums | `PascalCase` |
| Functions | `snake_case` |

---

## Key design principles

1. **Core is openpyxl-only** — validation, structure, metadata have zero Databricks dependency
2. **LLM adapters are optional extras** — `pip install databricks-excel-ingest-framework[databricks]`
3. **Canonical dictionary is caller-supplied** — no hardcoded domain fields in the package
4. **No Delta Lake writes** — package returns dataclasses/dicts; caller handles persistence
5. **Databricks is primary** — Unity Catalog Volumes, DBFS, `abfss://` path detection built-in
6. **Works outside Databricks** — openpyxl path works on any Python 3.9+ environment
7. **Parameterised LLM** — all adapter model names are constructor parameters with sensible defaults

---

## Confidence scoring

```
final_confidence = 0.7 * rule_score + 0.3 * llm_confidence   # LLM enabled
final_confidence = rule_score                                   # LLM disabled (adapter=None)

Thresholds: > 0.9 → AUTO_APPROVED | 0.7–0.9 → NEEDS_REVIEW | < 0.7 → REQUIRES_HUMAN
```

---

## Quick usage reference

```python
from excel_ingest import ExcelIngestFramework
from excel_ingest.mapping.adapters.databricks import DatabricksAdapter

# Optional: attach an LLM adapter
adapter = DatabricksAdapter(model="databricks-llama-3-70b-instruct")

framework = ExcelIngestFramework(spark=spark, adapter=adapter)

# Full pipeline in one call
result = framework.ingest(
    file_path="/Volumes/catalog/schema/volume/data.xlsx",
    canonical_dict={"order_id": ["order no", "transaction id"], ...},
    country_code="UK",
)

# Or stage by stage
validation  = framework.validate(file_path)
structure   = framework.detect_structure(file_path)
metadata    = framework.extract_metadata(file_path, structure)
mappings    = framework.map_to_canonical(metadata, canonical_dict)
```

---

## Package build and publish

```bat
build_and_publish.bat           # builds wheel + uploads to PyPI
python -m build                 # build only
python -m twine upload dist/*   # publish only
```

---

## PyPI details

- Distribution name : `databricks-excel-ingest-framework`
- Import name       : `excel_ingest`
- Version           : `0.1.0a14`
- PyPI name confirmed available: 2026-04-19
- Target repo       : `github.com/NitMatGeo/databricks-excel-ingest-framework`
