# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Install databricks-excel-ingest-framework
# MAGIC Install the package on the cluster. Run once per cluster restart.

# COMMAND ----------

# Core only (openpyxl — no LLM)
%pip install databricks-excel-ingest-framework==0.1.0a1

# With Databricks Foundation Models LLM adapter
# %pip install "databricks-excel-ingest-framework[databricks]==0.1.0a1"

# With all adapters
# %pip install "databricks-excel-ingest-framework[all]==0.1.0a1"

dbutils.library.restartPython()

# COMMAND ----------

import excel_ingest
print(excel_ingest.__version__)
