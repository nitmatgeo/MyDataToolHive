# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Stage 4: Canonical Mapping
# MAGIC Maps column headers to your canonical field names using rule-based scoring,
# MAGIC optionally boosted by a Databricks / OpenAI / Anthropic LLM adapter.

# COMMAND ----------

from excel_ingest import ExcelIngestFramework, MappingStatus

# ── Canonical dictionary: fully caller-supplied, any domain ───────────────
CANONICAL_DICT = {
    "employee_id":       ["emp id", "staff no", "employee number", "eid"],
    "first_name":        ["first name", "forename", "given name"],
    "last_name":         ["last name", "surname", "family name"],
    "email":             ["email address", "e-mail", "email"],
    "department":        ["dept", "department", "business unit"],
    "hire_date":         ["start date", "date of joining", "join date", "hire date"],
    # Add as many as needed for your domain
}

FILE_PATH = "/Volumes/my_catalog/my_schema/my_volume/data.xlsx"
FILE_ID   = "HR_2026_Q1"

# ── Option A: rule-only (no LLM, zero extra dependencies) ─────────────────
framework = ExcelIngestFramework(spark=spark)

# ── Option B: with Databricks LLM adapter ─────────────────────────────────
# from excel_ingest.mapping.adapters.databricks import DatabricksAdapter
# adapter = DatabricksAdapter(model="databricks-llama-3-70b-instruct")
# framework = ExcelIngestFramework(spark=spark, adapter=adapter)

# ── Option C: with OpenAI adapter ─────────────────────────────────────────
# from excel_ingest.mapping.adapters.openai import OpenAIAdapter
# adapter = OpenAIAdapter(model="gpt-4o-mini")
# framework = ExcelIngestFramework(spark=spark, adapter=adapter)

# COMMAND ----------

result = framework.ingest(
    file_path=FILE_PATH,
    canonical_dict=CANONICAL_DICT,
    file_id=FILE_ID,
    country_code="UK",
)

print(f"Success: {result.success}")
print()
for m in result.mappings:
    flag = "✓" if m.mapping_status == MappingStatus.AUTO_APPROVED else (
           "?" if m.mapping_status == MappingStatus.NEEDS_REVIEW else "✗"
    )
    print(
        f"  {flag} [{m.mapping_status.value:<16}] "
        f"conf={m.final_confidence:.2f}  "
        f"{m.hierarchical_header}  →  {m.canonical_field or 'UNMAPPED'}"
    )

# COMMAND ----------

# Persist mapping results to Delta
# records = result.mapping_records()
# spark.createDataFrame(records).write.mode("append").saveAsTable("my_catalog.my_schema.canonical_mappings")
