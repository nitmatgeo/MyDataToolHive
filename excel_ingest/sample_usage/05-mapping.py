# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Stage 4: Canonical Mapping on Sample Files
# MAGIC
# MAGIC Runs `framework.ingest()` (full pipeline) on representative sample files using a
# MAGIC FreshMart retail canonical dictionary. Shows confidence scores and mapping status
# MAGIC for each column. Demonstrates rule-only, LLM-assisted, and multi-sheet scenarios.

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

# DBTITLE 1,Canonical Dictionary — FreshMart retail domain
# Fully caller-supplied — the framework has no hardcoded domain knowledge.
# Keys are your target field names; values are the aliases you expect in source files.

CANONICAL_DICT = {
    "order_id":          ["order id", "order no", "order number", "ord id", "transaction id"],
    "order_date":        ["order date", "date", "transaction date", "sale date"],
    "ship_date":         ["ship date", "shipped date", "dispatch date"],
    "delivery_date":     ["delivery date", "delivered date", "actual delivery"],
    "order_status":      ["order status", "status"],
    "order_channel":     ["order channel", "channel", "sales channel"],
    "customer_id":       ["customer id", "customer no", "cust id", "client id"],
    "customer_name":     ["customer name", "customer", "client name", "buyer"],
    "customer_email":    ["customer email", "email", "e-mail"],
    "customer_segment":  ["customer segment", "segment", "customer type"],
    "loyalty_tier":      ["loyalty tier", "tier", "loyalty level"],
    "product_id":        ["product id", "product no", "item id", "sku"],
    "product_name":      ["product name", "product", "item name", "description"],
    "brand":             ["brand", "brand name", "manufacturer"],
    "category":          ["category", "product category", "dept"],
    "sub_category":      ["sub-category", "sub category", "subcategory"],
    "quantity":          ["quantity", "qty", "units", "stock units"],
    "unit_price":        ["unit price", "price", "rrp", "selling price"],
    "cost_price":        ["cost price", "cost", "buy price"],
    "discount":          ["discount", "discount %", "discount rate"],
    "gross_amount":      ["gross amount", "total", "gross total", "amount"],
    "net_amount":        ["net amount", "net total", "net"],
    "margin_pct":        ["margin %", "margin", "gross margin %"],
    "currency":          ["currency", "currency code"],
    "store_id":          ["store id", "store no", "shop id"],
    "store_name":        ["store name", "store", "shop name", "retail unit"],
    "store_type":        ["store type", "format", "store format"],
    "city":              ["city", "town", "location"],
    "country":           ["country", "country code", "nation"],
    "region":            ["region", "territory", "area"],
    "sales_rep":         ["sales rep", "sales rep name", "rep", "account manager"],
    "supplier_name":     ["supplier", "supplier name", "vendor"],
    "country_of_origin": ["country of origin", "origin", "source country"],
}

# COMMAND ----------

# DBTITLE 1,Initialise Framework (rule-only — no LLM)
from excel_ingest import ExcelIngestFramework
from excel_ingest.structure import FileProcessingConfig

framework = ExcelIngestFramework(spark=spark)

# To enable LLM-assisted mapping, uncomment one of:
# from excel_ingest.mapping.adapters.databricks import DatabricksAdapter
# framework = ExcelIngestFramework(spark=spark, adapter=DatabricksAdapter())

# from excel_ingest.mapping.adapters.openai import OpenAIAdapter
# framework = ExcelIngestFramework(spark=spark, adapter=OpenAIAdapter())

# COMMAND ----------

# DBTITLE 1,Mapping Status Reference
# mapping_status values in the output tables below — what each means and what action is needed.
# requires_action=True means a human must review the column before it can be loaded safely.

from excel_ingest import MappingStatus

display(spark.createDataFrame([
    {"mapping_status": s.value, "description": s.description, "requires_action": s.requires_action}
    for s in MappingStatus
]))

# COMMAND ----------

# DBTITLE 1,S01 — Simple single sheet

result_s01 = framework.ingest(
    file_path=f"{VOLUME_PATH}/s01_simple_single_sheet.xlsx",
    canonical_dict=CANONICAL_DICT,
    file_id="S01",
    country_code="UK",
)
display(spark.createDataFrame([result_s01.summary_record()]))
display(spark.createDataFrame(result_s01.mapping_records()))

# COMMAND ----------

# DBTITLE 1,S02 — Multi-row merged headers (Product Catalogue)

result_s02 = framework.ingest(
    file_path=f"{VOLUME_PATH}/s02_multi_row_merged_headers.xlsx",
    canonical_dict=CANONICAL_DICT,
    file_id="S02",
    config=FileProcessingConfig(sheet_name="Product Catalogue", static_header_rows=[1, 2]),
)
display(spark.createDataFrame([result_s02.summary_record()]))
display(spark.createDataFrame(result_s02.mapping_records()))

# COMMAND ----------

# DBTITLE 1,S07 — Wide Extended sheet (65 cols, 7 merged sections)

result_s07 = framework.ingest(
    file_path=f"{VOLUME_PATH}/s07_wide_standard_vs_extended.xlsx",
    canonical_dict=CANONICAL_DICT,
    file_id="S07_EXT",
    config=FileProcessingConfig(sheet_name="Extended", static_header_rows=[1, 2]),
)
display(spark.createDataFrame([result_s07.summary_record()]))
display(spark.createDataFrame(result_s07.mapping_records()))

# COMMAND ----------

# DBTITLE 1,S11 — Password protected

result_s11 = framework.ingest(
    file_path=f"{VOLUME_PATH}/s11_password_protected.xlsx",
    canonical_dict=CANONICAL_DICT,
    file_id="S11",
    password="Password1234",
)
display(spark.createDataFrame([result_s11.summary_record()]))
display(spark.createDataFrame(result_s11.mapping_records()))

# COMMAND ----------

# DBTITLE 1,S12 — Wide 3-level merged headers — UK sheet (45 cols)

result_s12 = framework.ingest(
    file_path=f"{VOLUME_PATH}/s12_wide_complex_3level_headers.xlsx",
    canonical_dict=CANONICAL_DICT,
    file_id="S12_UK",
    config=FileProcessingConfig(sheet_name="UK", static_header_rows=[1, 2, 3]),
    country_code="UK",
)
display(spark.createDataFrame([result_s12.summary_record()]))
display(spark.createDataFrame(result_s12.mapping_records()))

# COMMAND ----------

# DBTITLE 1,All Files — Summary across all 5 files
# One row per file — auto_approved / needs_review / requires_human / unmapped counts.
# requires_action columns in the per-file detail tables flag columns needing human attention.

all_results = [result_s01, result_s02, result_s07, result_s11, result_s12]
display(spark.createDataFrame([r.summary_record() for r in all_results]))

# COMMAND ----------

# DBTITLE 1,All Files — Combined mapping detail (scrollable)
# One row per column across all 5 files.
# Filter by file_id, mapping_status, or requires_action=True to find columns needing review.

all_records = [rec for r in all_results for rec in r.mapping_records()]
display(spark.createDataFrame(all_records))

# COMMAND ----------

# DBTITLE 1,(Optional) Persist mapping records to Delta

# spark.createDataFrame(all_records).write \
#     .mode("append") \
#     .saveAsTable(f"{MY_CATALOG}.{INGEST_SCHEMA}.excel_canonical_mappings")
