# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Stage 3: Extract Metadata from All Sample Files
# MAGIC
# MAGIC Runs `framework.extract_metadata()` on every sample file to build hierarchical
# MAGIC column headers, section IDs, and SHA-256 layout signatures. Demonstrates how
# MAGIC identical layouts across multiple files produce matching signatures.

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
from excel_ingest import ExcelIngestFramework, combine_column_records
from excel_ingest.structure import FileProcessingConfig

framework = ExcelIngestFramework(spark=spark)

# COMMAND ----------

# DBTITLE 1,File Configs (same sheet/header hints as 03-structure)
FILE_CONFIGS = [
    {"file": "s01_simple_single_sheet.xlsx",         "id": "S01", "password": None,           "config": FileProcessingConfig()},
    {"file": "s02_multi_row_merged_headers.xlsx",    "id": "S02", "password": None,           "config": FileProcessingConfig(sheet_name="Product Catalogue", static_header_rows=[1, 2])},
    {"file": "s03_no_headers.xlsx",                  "id": "S03", "password": None,           "config": FileProcessingConfig(data_start_row=1)},
    {"file": "s04_headers_only_no_data.xlsx",        "id": "S04", "password": None,           "config": FileProcessingConfig()},
    {"file": "s05_multi_sheet_diff_structure.xlsx",  "id": "S05", "password": None,           "config": FileProcessingConfig(sheet_name="Orders")},
    {"file": "s06_multi_sheet_same_structure.xlsx",  "id": "S06", "password": None,           "config": FileProcessingConfig(sheet_name="UK")},
    {"file": "s07_wide_standard_vs_extended.xlsx",   "id": "S07", "password": None,           "config": FileProcessingConfig(sheet_name="Extended", static_header_rows=[1, 2])},
    {"file": "s08_hidden_sheet.xlsx",                "id": "S08", "password": None,           "config": FileProcessingConfig(sheet_name="Sales Report")},
    {"file": "s09_hidden_columns.xlsx",              "id": "S09", "password": None,           "config": FileProcessingConfig()},
    {"file": "s10_blank_column_sections.xlsx",       "id": "S10", "password": None,           "config": FileProcessingConfig(static_header_rows=[1, 2])},
    {"file": "s11_password_protected.xlsx",          "id": "S11", "password": "Password1234", "config": FileProcessingConfig()},
    {"file": "s12_wide_complex_3level_headers.xlsx", "id": "S12", "password": None,           "config": FileProcessingConfig(sheet_name="UK", static_header_rows=[1, 2, 3])},
]

# COMMAND ----------

# DBTITLE 1,Extract Metadata for Each File

all_metadata = []

for cfg in FILE_CONFIGS:
    path      = f"{VOLUME_PATH}/{cfg['file']}"
    structure = framework.detect_structure(path, config=cfg["config"], password=cfg["password"])
    metadata  = framework.extract_metadata(path, structure, file_id=cfg["id"])

    all_metadata.append(metadata)
    fm = metadata.file_metadata

    print(f"[{cfg['id']}] {cfg['file']}  |  sheet: {structure.sheet_name}")
    print(f"       Signature    : {fm.header_signature[:24]}...")
    print(f"       Sections     : {fm.num_sections}")
    print(f"       Total cols   : {len(metadata.column_metadata)}")
    print(f"       Merged       : {fm.num_merged_regions}")
    print()
    for col in metadata.column_metadata[:6]:    # first 6 columns only
        flags = ("BLANK " if col.is_blank_column else "") + ("MERGE " if col.is_part_of_merge else "")
        print(f"         col {col.column_index:>3} ({col.column_letter})  sec={col.section_id}  {flags}{col.hierarchical_header}")
    if len(metadata.column_metadata) > 6:
        print(f"         ... {len(metadata.column_metadata) - 6} more columns")
    print()

# COMMAND ----------

# DBTITLE 1,Full Column Listing — all files (scrollable table)
# combine_column_records() flattens all metadata into one list ready for display().
# Filter by file_id or column_group to inspect wide files (S07: 65 cols, S12: 45 cols).

display(spark.createDataFrame(combine_column_records(all_metadata)))

# COMMAND ----------

# DBTITLE 1,Multi-Sheet Iteration — extract metadata for every sheet in a file
# validate() gives the sheet list — no hardcoding needed.
# schema_match: sheets sharing a signature have identical column layouts.
# S05 → 3 sheets, all different structures → all UNIQUE
# S06 → 4 regional sheets, identical structure → UK is UNIQUE, US/DE/AU are MATCH

ms_sheet_records = []
ms_col_records   = []

for fname, fid in [
    ("s05_multi_sheet_diff_structure.xlsx", "S05"),
    ("s06_multi_sheet_same_structure.xlsx", "S06"),
]:
    path   = f"{VOLUME_PATH}/{fname}"
    sheets = framework.validate(path).visible_sheet_names

    for sheet in sheets:
        config    = FileProcessingConfig(sheet_name=sheet)
        structure = framework.detect_structure(path, config=config)
        meta      = framework.extract_metadata(path, structure, file_id=fid)

        ms_sheet_records.append(meta.signature_record())
        ms_col_records.extend(meta.column_records())

# Compute schema_group — first sheet in workbook tab order with a given signature anchors the group.
# All sheets sharing that signature get schema_group = that first sheet's name.
# is_header_unique = True only for the anchor sheet (first occurrence).
_seen = {}
for rec in ms_sheet_records:
    anchor = _seen.setdefault(rec["header_signature"], rec["sheet_name"])
    rec["schema_group"]      = anchor
    rec["is_header_unique"]  = anchor == rec["sheet_name"]

print("Sheet summary:")
display(spark.createDataFrame(ms_sheet_records))

print("Column listing:")
display(spark.createDataFrame(ms_col_records))

# COMMAND ----------

# DBTITLE 1,Signature Comparison — detect matching layouts
# signature_record() = file_id, file_name, sheet_name, total_cols, header_signature.
# Files with the same header_signature have identical column layouts.
# Store this table after every ingest run — compare on the next run to detect schema drift.

display(spark.createDataFrame([m.signature_record() for m in all_metadata]))

# COMMAND ----------

# DBTITLE 1,(Optional) Persist schema signatures to a reference table
# signature_record() returns file_id, file_name, sheet_name, total_cols, header_signature.
# Store this after every ingest run — compare on the next run to detect schema drift.

# spark.createDataFrame([m.signature_record() for m in all_metadata]).write \
#     .mode("append") \
#     .saveAsTable(f"{MY_CATALOG}.{INGEST_SCHEMA}.excel_schema_signatures")

# COMMAND ----------

# DBTITLE 1,(Optional) Persist column metadata to Delta

# records = []
# for meta in all_metadata:
#     records.extend(meta.to_delta_records())
#
# spark.createDataFrame(records).write \
#     .mode("append") \
#     .saveAsTable(f"{MY_CATALOG}.{INGEST_SCHEMA}.excel_column_metadata")
