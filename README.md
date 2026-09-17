# MyDataToolHive

A collection of open-source Python frameworks for Databricks — data quality assessment, ETL pipeline monitoring, and Excel ingestion into Delta Lake.

[![GitHub](https://img.shields.io/badge/GitHub-MyDataToolHive-blue?logo=github)](https://github.com/nitmatgeo/MyDataToolHive)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-nitmatgeo-0A66C2?logo=linkedin)](https://linkedin.com/in/nitmatgeo)

---

## Articles

- [🧰 DataToolHive | Excel files in Databricks — the combinations that break everything, and a framework built for all of them](https://www.linkedin.com/pulse/datatoolhive-databricks-excel-ingest-framework-nitin-mathew-george-1jlic) — LinkedIn article on `databricks-excel-ingest-framework`
- [🧰 DataToolHive | Data quality has two very different problems. This framework solves one of them.](https://www.linkedin.com/pulse/datatoolhive-databricks-dq-framework-nitmatgeo-nitin-mathew-george-u8hjc) — LinkedIn article on `databricks-dq-framework`
- [🧰 DataToolHive | databricks-etl-monitor — pipeline control without touching the pipeline](https://www.linkedin.com/pulse/datatoolhive-databricks-etl-monitor-nitmatgeo-nitin-mathew-george-yoonc) — LinkedIn article on `databricks-etl-monitor`
- [🧰 DataToolHive | SQL Server Schema Craft Studio — column-level schema change tracking, version to version](https://www.linkedin.com/pulse/datatoolhive-sql-server-schema-craft-studio-nitmatgeo-mathew-george-dlrwc) — LinkedIn article on SQL Server Schema Craft Studio

---

## Packages

### Databricks Python Packages (PyPI)

| Package | PyPI | Description |
|---------|------|-------------|
| [databricks-dq-framework](./databricks-dq-framework/) | [![PyPI](https://img.shields.io/pypi/v/databricks-dq-framework)](https://pypi.org/project/databricks-dq-framework/) | Field-level data quality assessment on Unity Catalog Delta tables |
| [databricks-etl-monitor](./databricks-etl-monitor/) | [![PyPI](https://img.shields.io/pypi/v/databricks-etl-monitor)](https://pypi.org/project/databricks-etl-monitor/) | ETL pipeline orchestration and monitoring framework |
| [databricks-excel-ingest-framework](./databricks-excel-ingest-framework/) | [![PyPI](https://img.shields.io/pypi/v/databricks-excel-ingest-framework)](https://pypi.org/project/databricks-excel-ingest-framework/) | Structured Excel-to-Delta ingestion with canonical field mapping |

### SQL Server Tools (PowerShell)

| Tool | Description |
|------|-------------|
| [SQL Server Schema Craft Studio](./SQL%20Server%20Schema%20Craft%20Studio/) | Schema metadata extraction, version comparison, and drift classification for SQL Server databases |

---

## databricks-dq-framework

Assesses data quality on Unity Catalog Delta tables by running configurable field-level checks — length, range, pattern, and custom SQL/regex/Python validators. Violations are recorded with full row-level audit; it does **not** transform or fix data.

```python
%pip install databricks-dq-framework --upgrade

from dq_framework import DQFramework
dq = DQFramework(spark, catalog="main", schema="dq")
dq.setup()
dq.config.register_field("email_address", data_category_type_id=5, field_id=1) \
         .set_field_values("email_address", config_id=2, min_data_length=6, max_data_length=254) \
         .block_category("email_address", "SpecialCharacter") \
         .allow_pattern("email_address", "Has At Sign") \
         .add_mapping("email_address", target_schema_name="Curated",
                      target_table_name="Customer", target_field_name="EmailAddress", mapping_id=5)
dq.generate_rule_functions()
dq.prepare_curated_tables()
exec_id = dq.run_assessment(schema_name="Curated")
dq.violations(exec_id).display()
```

[Full documentation →](./databricks-dq-framework/README.md)

---

## databricks-etl-monitor

ETL pipeline orchestration and monitoring framework for Databricks. Manages workflow configuration, sequence stages, file-based task scheduling, and pipeline health tracking across multi-org enterprise hierarchies.

```python
%pip install databricks-etl-monitor --upgrade

from etl_monitor import ETLMonitor
monitor = ETLMonitor(spark, catalog="main", schema="etl")
monitor.setup()
```

[Full documentation →](./databricks-etl-monitor/README.md)

---

## databricks-excel-ingest-framework

Structured ingestion of Excel files into Databricks Delta Lake. Validates file integrity, detects sheet structure, extracts column metadata, maps headers to a caller-supplied canonical schema (with optional LLM assistance), and loads data as a Spark DataFrame.

```python
%pip install databricks-excel-ingest-framework --upgrade

from excel_ingest import ExcelIngestFramework
framework = ExcelIngestFramework(spark=spark)

result = framework.ingest(
    file_path="/Volumes/<catalog>/<schema>/<volume>/orders.xlsx",
    canonical_dict={
        "order_id":     ["order id", "order no", "transaction id"],
        "product_name": ["product name", "item name", "description"],
    },
    country_code="UK",
    file_id="ORDERS_2026_Q1",
)
display(spark.createDataFrame([result.summary_record()]))
```

[Full documentation →](./databricks-excel-ingest-framework/README.md)

---

## SQL Server Schema Craft Studio

A modular PowerShell toolkit for SQL Server schema management. Built for data engineers and DBAs who need to track what changed between database versions — before it silently breaks a pipeline.

**What it does:**
- **Extracts** schema metadata from a SQL Server database and serialises it to JSON (`SchemaMetadataExtract.ps1`)
- **Compares** two schema snapshots (version n vs n−1) and produces a detailed change log (`SchemaMetadataComparator.ps1`)
- **Classifies** every table as Newly Added, Deleted, Unchanged, or Changed — and outputs a summary CSV (`SchemaMetadataSummary.ps1`)

**When to use it:**
- Before and after a database deployment — catch column additions, removals, or renames before they surface as pipeline failures
- As part of a CI gate — run the comparator against a baseline snapshot and fail the build if breaking changes appear
- Audit trail — keep versioned JSON snapshots of your schema alongside your code

**Run the full workflow in one command:**

```powershell
.\Main.ps1 `
  -rootPath "C:\SchemaSnapshots" `
  -versionN "v2" `
  -versionNMinus1 "v1" `
  -rawFileName "00.rawDumpOutput.json" `
  -comparisonLogFile "DetailedComparisonLog.csv" `
  -summaryOutputFile "TableClassificationOutput.csv" `
  -debugMode $true
```

[Full documentation →](./SQL%20Server%20Schema%20Craft%20Studio/README.md)

---

## Repository structure

```
MyDataToolHive/
├── databricks-dq-framework/          # DQ assessment package
│   ├── dq_framework/
│   ├── pyproject.toml
│   └── README.md
├── databricks-etl-monitor/           # ETL monitoring package
│   ├── etl_monitor/
│   ├── pyproject.toml
│   └── README.md
├── databricks-excel-ingest-framework/ # Excel ingestion package
│   ├── excel_ingest/
│   ├── pyproject.toml
│   └── README.md
├── SQL Server Schema Craft Studio/   # SQL Server schema drift tool (PowerShell)
│   ├── Modules/
│   ├── Scripts/
│   ├── msql.PowerShell.SCS.Main.ps1
│   └── README.md
└── .github/
    ├── copilot-instructions.md        # Repo-level Copilot context
    ├── instructions/                  # Per-package Copilot instructions
    └── workflows/                     # CI/CD — auto-publish to PyPI on push
```

---

## CI/CD

Each package publishes to PyPI automatically when files in its subfolder change on `main`. Publishing uses [PyPI Trusted Publishing (OIDC)](https://docs.pypi.org/trusted-publishers/) — no API tokens required.

| Workflow | Trigger path |
|----------|-------------|
| `publish-dq-framework.yml` | `databricks-dq-framework/**` |
| `publish-etl-monitor.yml` | `databricks-etl-monitor/**` |
| `publish-excel-ingest.yml` | `databricks-excel-ingest-framework/**` |

---

## License

MIT
