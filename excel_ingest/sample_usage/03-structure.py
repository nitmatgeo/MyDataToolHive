# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Stage 2: Detect Structure of All Sample Files
# MAGIC
# MAGIC Runs `framework.detect_structure()` on every sample file. Each file has a tailored
# MAGIC `FileProcessingConfig` — multi-sheet files specify the target sheet, files with known
# MAGIC static headers bypass auto-detection.

# COMMAND ----------

# DBTITLE 1,Install & Inherit Variables
%run ./01-install

# COMMAND ----------

# DBTITLE 1,Config
MY_CATALOG    = "sampledatacatalog"
INGEST_SCHEMA = "bronze"
VOLUME_NAME   = "excel_ingest_samples"
VOLUME_PATH   = f"/Volumes/{MY_CATALOG}/{INGEST_SCHEMA}/{VOLUME_NAME}"

# COMMAND ----------

# DBTITLE 1,Initialise Framework
from excel_ingest import ExcelIngestFramework
from excel_ingest.structure import FileProcessingConfig

framework = ExcelIngestFramework(spark=spark)

# COMMAND ----------

# DBTITLE 1,File Configs — sheet + header hints per file
# sheet_name=None  → auto-select (only works for single-sheet files)
# static_header_rows → skip auto-detection when row numbers are known
FILE_CONFIGS = [
    # Auto-detect works for single-header-row files — no static_header_rows needed
    {"file": "s01_simple_single_sheet.xlsx",
     "label": "Simple single sheet",
     "password": None,
     "config": FileProcessingConfig()},

    # Multi-row headers: provide static_header_rows so the framework knows all header rows
    {"file": "s02_multi_row_merged_headers.xlsx",
     "label": "Multi-row merged headers",
     "password": None,
     "config": FileProcessingConfig(sheet_name="Product Catalogue", static_header_rows=[1, 2])},

    # No headers: data_start_row=1 tells the framework to treat everything as data
    {"file": "s03_no_headers.xlsx",
     "label": "No headers (raw data)",
     "password": None,
     "config": FileProcessingConfig(data_start_row=1)},

    {"file": "s04_headers_only_no_data.xlsx",
     "label": "Headers only, no data",
     "password": None,
     "config": FileProcessingConfig()},

    {"file": "s05_multi_sheet_diff_structure.xlsx",
     "label": "Multi-sheet diff structure — Orders sheet",
     "password": None,
     "config": FileProcessingConfig(sheet_name="Orders")},

    {"file": "s06_multi_sheet_same_structure.xlsx",
     "label": "Multi-sheet same structure — UK sheet",
     "password": None,
     "config": FileProcessingConfig(sheet_name="UK")},

    {"file": "s07_wide_standard_vs_extended.xlsx",
     "label": "Wide Standard sheet (15 cols)",
     "password": None,
     "config": FileProcessingConfig(sheet_name="Standard")},

    {"file": "s07_wide_standard_vs_extended.xlsx",
     "label": "Wide Extended sheet (65 cols, merged headers)",
     "password": None,
     "config": FileProcessingConfig(sheet_name="Extended", static_header_rows=[1, 2])},

    {"file": "s08_hidden_sheet.xlsx",
     "label": "Hidden sheet present — Sales Report",
     "password": None,
     "config": FileProcessingConfig(sheet_name="Sales Report")},

    {"file": "s09_hidden_columns.xlsx",
     "label": "Hidden columns (F, I)",
     "password": None,
     "config": FileProcessingConfig()},

    {"file": "s10_blank_column_sections.xlsx",
     "label": "Blank column separators",
     "password": None,
     "config": FileProcessingConfig(static_header_rows=[1, 2])},

    {"file": "s11_password_protected.xlsx",
     "label": "Password protected",
     "password": "Password1234",
     "config": FileProcessingConfig()},

    {"file": "s12_wide_complex_3level_headers.xlsx",
     "label": "Wide 3-level merged headers — UK sheet (45 cols)",
     "password": None,
     "config": FileProcessingConfig(sheet_name="UK", static_header_rows=[1, 2, 3])},
]

# COMMAND ----------

# DBTITLE 1,Detect Structure for Each File
# One row per file — sortable, filterable, consistent with later stages.
# status_description explains what was found; is_actionable = True means the pipeline
# cannot proceed — reconfigure FileProcessingConfig and re-run.

all_structure = []

for cfg in FILE_CONFIGS:
    path = f"{VOLUME_PATH}/{cfg['file']}"
    s    = framework.detect_structure(path, config=cfg["config"], password=cfg["password"])
    all_structure.append(s.summary_record(file_path=cfg["file"], label=cfg["label"]))

display(spark.createDataFrame(all_structure))

# COMMAND ----------

# DBTITLE 1,Files Needing Action
# Rows where is_actionable = True must be resolved before metadata extraction can proceed.
# Read status_description for the recommended fix.

spark.createDataFrame(all_structure).filter("is_actionable = true").display()
