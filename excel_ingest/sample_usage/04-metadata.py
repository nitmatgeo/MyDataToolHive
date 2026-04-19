# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Stage 3: Extract Metadata
# MAGIC Builds hierarchical column headers, assigns section IDs, generates SHA-256 signature.

# COMMAND ----------

from excel_ingest import ExcelIngestFramework

framework = ExcelIngestFramework(spark=spark)
FILE_PATH  = "/Volumes/my_catalog/my_schema/my_volume/data.xlsx"
FILE_ID    = "HR_2026_Q1"   # any unique identifier for this file load

structure = framework.detect_structure(FILE_PATH)
metadata  = framework.extract_metadata(FILE_PATH, structure, file_id=FILE_ID)

fm = metadata.file_metadata
print(f"File ID         : {fm.file_id}")
print(f"Signature       : {fm.header_signature}")
print(f"Sections        : {fm.num_sections}")
print(f"Merged regions  : {fm.num_merged_regions}")
print()

for col in metadata.column_metadata:
    print(
        f"  Col {col.column_index:>3} ({col.column_letter})  "
        f"section={col.section_id}  "
        f"{'BLANK ' if col.is_blank_column else ''}"
        f"{'MERGE ' if col.is_part_of_merge else ''}"
        f"{col.hierarchical_header}"
    )

# Optionally write to Delta
# records = metadata.to_delta_records()
# spark.createDataFrame(records).write.mode("append").saveAsTable("my_catalog.my_schema.column_metadata")
