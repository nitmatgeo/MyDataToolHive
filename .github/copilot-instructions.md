# MyDataToolHive — Repository Context

**Repository:** https://github.com/nitmatgeo/MyDataToolHive  
**Author:** Nitin Mathew George (NitMatGeo)

A monorepo of independent data engineering tools for Databricks and SQL Server, each independently deployable as a Python package or PowerShell module suite.

---

## Subprojects

| Folder | Package / Module | PyPI / Purpose |
|--------|-----------------|----------------|
| `databricks-dq-framework/` | `databricks-dq-framework` | Metadata-driven Data Quality Assessment for Databricks Delta tables |
| `databricks-etl-monitor/` | `databricks-etl-monitor` | ETL Process Monitoring Framework for Databricks / ADF pipelines |
| `databricks-excel-ingest-framework/` | `databricks-excel-ingest-framework` | Excel-to-Delta ingestion pipeline (validate → structure → metadata → load → map) |
| `SQL Server Schema Craft Studio/` | PowerShell module suite | Schema metadata extraction, comparison, and summary for SQL Server |

Each subfolder is **independently deployable** and has its own `README.md`, `CLAUDE.md`, and `pyproject.toml` (for Python packages).

---

## Per-subproject Copilot instructions

Scoped instruction files live in `.github/instructions/` and are loaded automatically by VS Code Copilot based on the files you are editing:

| Instruction file | Applies to |
|-----------------|-----------|
| `dq-framework.instructions.md` | `databricks-dq-framework/**` |
| `etl-monitor.instructions.md` | `databricks-etl-monitor/**` |
| `excel-ingest.instructions.md` | `databricks-excel-ingest-framework/**` |

---

## Shared conventions across all Python subprojects

- **Databricks SQL only** — `CREATE TABLE IF NOT EXISTS`, `USING DELTA`, backtick-quoted FQNs.
- **PascalCase** for all Delta table and column names.
- **INSERT-ONLY MERGE** for config writes — `WHEN NOT MATCHED THEN INSERT`.
- **COALESCE in MERGE ON** for nullable string keys.
- **`pyproject.toml` only** — no `setup.py`. Build backend: `setuptools.build_meta`.
- **`<catalog>` / `<schema>` placeholders** in all SQL examples — never hardcode real names.
- **Version lives only in `pyproject.toml`** — `__init__.py` reads it via `importlib.metadata`.
- **No Delta writes inside packages** — result objects return dicts; callers write to Delta.

## Package build

```bash
python -m build
# or
build_and_publish.bat
```

## Git

- Main branch: `main`
- Git user: `NitMatGeo`
