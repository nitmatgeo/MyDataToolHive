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
# Filter by file_id or header_section to inspect wide files (S07: 65 cols, S12: 45 cols).

display(spark.createDataFrame(combine_column_records(all_metadata)))

# COMMAND ----------

# DBTITLE 1,Multi-Sheet Iteration — extract metadata for every sheet in a file
# Demonstrates how to iterate all visible sheets without hardcoding sheet names.
# validate() gives you the sheet list; detect_structure() + extract_metadata() run per sheet.
#
# S05 → 3 sheets with different structures → unique signature per sheet
# S06 → 4 regional sheets with identical structure → all signatures match

from excel_ingest.structure import FileProcessingConfig

for fname, fid in [
    ("s05_multi_sheet_diff_structure.xlsx", "S05"),
    ("s06_multi_sheet_same_structure.xlsx", "S06"),
]:
    path       = f"{VOLUME_PATH}/{fname}"
    validation = framework.validate(path)
    sheets     = validation.visible_sheet_names

    print(f"\n{fname}  ({len(sheets)} visible sheets)")
    sheet_sigs = {}

    for sheet in sheets:
        config   = FileProcessingConfig(sheet_name=sheet)
        structure = framework.detect_structure(path, config=config)
        metadata  = framework.extract_metadata(path, structure, file_id=f"{fid}_{sheet}")

        sig = metadata.file_metadata.header_signature[:16]
        match = next((s for s, v in sheet_sigs.items() if v == sig), None)
        tag   = f"MATCH ({match})" if match else "UNIQUE"
        sheet_sigs[sheet] = sig

        print(f"  [{tag:<20}]  sheet={sheet:<20}  cols={metadata.file_metadata.total_cols:<4}  sig={sig}...")
        for col in metadata.column_metadata[:4]:
            print(f"             {col.column_letter:<4} {col.hierarchical_header}")
        if len(metadata.column_metadata) > 4:
            print(f"             ... {len(metadata.column_metadata) - 4} more columns")

# COMMAND ----------

# DBTITLE 1,Signature Comparison — detect matching layouts

print("Signature comparison (files with identical layouts share a signature):\n")
seen = {}
for cfg, meta in zip(FILE_CONFIGS, all_metadata):
    sig = meta.file_metadata.header_signature[:16]
    if sig in seen:
        print(f"  MATCH  {cfg['file']} == {seen[sig]}")
    else:
        seen[sig] = cfg['file']
        print(f"  UNIQUE {cfg['file']}  [{sig}...]")

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
