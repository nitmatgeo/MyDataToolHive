# MyDataToolHive

A collection of open-source Python frameworks for Databricks — data quality assessment, ETL pipeline monitoring, and Excel ingestion into Delta Lake.

[![GitHub](https://img.shields.io/badge/GitHub-MyDataToolHive-blue?logo=github)](https://github.com/nitmatgeo/MyDataToolHive)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-nitmatgeo-0A66C2?logo=linkedin)](https://linkedin.com/in/nitmatgeo)

---

## Articles

- [🧰 DataToolHive | Excel files in Databricks — the combinations that break everything, and a framework built for all of them](https://www.linkedin.com/pulse/datatoolhive-excel-files-databricks-combinations-all-mathew-george-igfpc) — LinkedIn article on `databricks-excel-ingest-framework`
- [🧰 DataToolHive | Data quality has two very different problems. This framework solves one of them.](https://www.linkedin.com/pulse/datatoolhive-data-quality-has-two-very-different-one-mathew-george-w0z9c) — LinkedIn article on `databricks-dq-framework`

---

## Packages

| Package | PyPI | Description |
|---------|------|-------------|
| [databricks-dq-framework](./databricks-dq-framework/) | [![PyPI](https://img.shields.io/pypi/v/databricks-dq-framework)](https://pypi.org/project/databricks-dq-framework/) | Field-level data quality assessment on Unity Catalog Delta tables |
| [databricks-etl-monitor](./databricks-etl-monitor/) | [![PyPI](https://img.shields.io/pypi/v/databricks-etl-monitor)](https://pypi.org/project/databricks-etl-monitor/) | ETL pipeline orchestration and monitoring framework |
| [databricks-excel-ingest-framework](./databricks-excel-ingest-framework/) | [![PyPI](https://img.shields.io/pypi/v/databricks-excel-ingest-framework)](https://pypi.org/project/databricks-excel-ingest-framework/) | Structured Excel-to-Delta ingestion with canonical field mapping |

---

## databricks-dq-framework

Assesses data quality on Unity Catalog Delta tables by running configurable field-level checks — length, range, pattern, and custom SQL/regex/Python validators. Violations are recorded with full row-level audit; it does **not** transform or fix data.

```python
%pip install databricks-dq-framework --upgrade

from dq_framework import DQFramework
dq = DQFramework(spark, catalog="main", schema="dq")
dq.setup()
dq.config.register_field(1, "email_address", data_category_type_id=5) \
         .set_field_values(2, "email_address", min_data_length=6, max_data_length=254) \
         .block_category(3, "email_address", "SpecialCharacter") \
         .allow_pattern(4, "email_address", "Has At Sign") \
         .add_mapping(5, "email_address", target_schema_name="Curated",
                      target_table_name="Customer", target_field_name="EmailAddress")
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
