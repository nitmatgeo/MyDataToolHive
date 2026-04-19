# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Stage 2: Detect Excel Structure
# MAGIC Auto-detects header rows, merged cells, blank/hidden columns, data boundaries.

# COMMAND ----------

from excel_ingest import ExcelIngestFramework
from excel_ingest.structure import FileProcessingConfig, FileStatus

framework = ExcelIngestFramework(spark=spark)
FILE_PATH = "/Volumes/my_catalog/my_schema/my_volume/data.xlsx"

# Auto-detect (single-sheet or specify sheet_name for multi-sheet)
config = FileProcessingConfig(
    sheet_name="Sheet1",          # required for multi-sheet files
    # static_header_rows=[1, 2], # uncomment if headers are at known rows
)

structure = framework.detect_structure(FILE_PATH, config)

print(f"Status          : {structure.status.value}")
print(f"Sheet           : {structure.sheet_name}")
print(f"Header rows     : {structure.header_structure.header_row_indices if structure.header_structure else 'N/A'}")
print(f"Data start row  : {structure.header_structure.data_start_row if structure.header_structure else 'N/A'}")
print(f"Data rows       : {structure.data_row_count}")
print(f"Merged regions  : {len(structure.merged_cells)}")
print(f"Blank columns   : {structure.blank_column_indices}")
print(f"Hidden columns  : {structure.hidden_column_indices}")

assert structure.status == FileStatus.VALID, f"Structure issue: {structure.status.value}"
