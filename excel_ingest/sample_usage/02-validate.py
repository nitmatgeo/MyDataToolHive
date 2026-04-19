# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Stage 1: Validate Excel File
# MAGIC Checks file existence, format, password protection, and sheet list.

# COMMAND ----------

from excel_ingest import ExcelIngestFramework, ValidationStatus

framework = ExcelIngestFramework(spark=spark)
FILE_PATH = "/Volumes/my_catalog/my_schema/my_volume/data.xlsx"

result = framework.validate(FILE_PATH)

print(f"Status      : {result.status.value}")
print(f"Location    : {result.location_type.value}")
print(f"Format      : {result.format_type}")
print(f"Sheets      : {result.all_sheet_names}")
print(f"Protected   : {result.is_password_protected}")
if result.warnings:
    print(f"Warnings    : {result.warnings}")
if result.errors:
    print(f"Errors      : {result.errors}")

assert result.status != ValidationStatus.FAILED, "Validation failed — fix errors before proceeding."
