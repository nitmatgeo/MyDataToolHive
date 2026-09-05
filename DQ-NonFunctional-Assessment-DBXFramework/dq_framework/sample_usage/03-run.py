# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Run Assessment & View Results
# MAGIC Runs the DQ assessment against the curated schema and displays results.
# MAGIC Depends on `01-install` (framework init) and one of the `02-config-*` notebooks (config seeding).

# COMMAND ----------

# DBTITLE 1,Inherit Framework and Variables from 01-install
# MAGIC %run ./01-install

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-flight Checks

# COMMAND ----------

# DBTITLE 1,Display Data Quality Framework Configuration Summary
dq.config.show_config_summary()

# COMMAND ----------

# DBTITLE 1,Verify Config — Duplicate ID, Duplicate Rule and Referential Integrity Checks
# Checks: duplicate _ID values (causes MERGE errors), duplicate logical rules in
# configFieldAllowedPattern (contradictory IsPatternAllowed), and FK integrity
# (FullFieldName / PatternName references).
# Note: also called automatically by generate_rule_functions() — raises RuntimeError on failure.
result = dq.config.verify_config()
if result["ok"]:
    print("✓ Config verification passed — no issues found")
else:
    for issue in result["issues"]:
        print(f"  ✗ {issue}")

# COMMAND ----------

# DBTITLE 1,Display Data Quality Framework Guide and Usage Help
dq.guide()

# COMMAND ----------

# DBTITLE 1,Browse Sample Notebooks, Data Files and Excel Template
dq.sample_usage(spark)

# COMMAND ----------

# DBTITLE 1,Generate Data Quality Rule Functions
dq.generate_rule_functions()

# COMMAND ----------

# DBTITLE 1,Inspect Generated DQ Rules for Email Address Field
dq.inspect_checker('fn_DQ_email_address', show_all_patterns=True)

# COMMAND ----------

# DBTITLE 1,Validate Custom SQL Queries Before Running Assessment
ok = dq.validate_custom_queries_sql()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Assessment

# COMMAND ----------

# DBTITLE 1,Add Data Quality Columns to All Curated Tables
# Adds DQEligible, DQViolations, DQFields columns to all curated tables (idempotent — safe to run multiple times)
dq.prepare_curated_tables()

# COMMAND ----------

# DBTITLE 1,Run Data Quality Assessment and Write Results to Tables
# Runs the full assessment; writes results to auditDQChecks and statDQChecks
if ok:
    exec_id = dq.run_assessment(schema_name=CURATED_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Results

# COMMAND ----------

# DBTITLE 1,Display Data Quality Violations
# View violations for this run
dq.violations(exec_id).display()

# COMMAND ----------

# DBTITLE 1,Display Data Quality Scores per Field
# Quality scores per field
dq.quality_scores(exec_id).display()

# COMMAND ----------

# DBTITLE 1,Field Rule Summary — Config Audit
# Flat DataFrame of all active rules per field — export to Excel, pivot, filter
# Pass a logical field name, physical Schema.Table.Column, or omit for all fields
dq.field_rule_summary().display()

# COMMAND ----------

# DBTITLE 1,Summary by Violation Type
# Record counts grouped by field and violation type
dq.summary_by_violation_type(exec_id).display()

# COMMAND ----------

# DBTITLE 1,Summary by Table
# Summary by table
dq.summary_by_table(exec_id).display()

# COMMAND ----------

# DBTITLE 1,Display Fields Below Quality Threshold
# Fields below a quality threshold (e.g. 80%)
dq.fields_below_threshold(threshold=80).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (Optional)
# MAGIC Run the cell below **only** if you want to tear down all sample and DQ tables/views from this demo.

# COMMAND ----------

# DBTITLE 1,Teardown Step 1 — Drop Tables and Views
list_tables = [
    "mock_curated_contacts",
    "mock_curated_locations",
    "mock_curated_vendors",
    "masterDataCategory",
    "masterPattern",
    "masterField",
    "configFieldValues",
    "configFieldAllowedPattern",
    "configCustomQuery",
    "mapDQChecks",
    "auditDQChecks",
    "statDQChecks",
]
list_views = [
    "v_auditDQChecks",
    "v_statDQChecks",
]

for table in list_tables:
    spark.sql(f"DROP TABLE IF EXISTS `{MY_CATALOG}`.`{DQ_SCHEMA}`.`{table}`")
    print(f"  dropped table  : {table}")
for view in list_views:
    spark.sql(f"DROP VIEW IF EXISTS `{MY_CATALOG}`.`{DQ_SCHEMA}`.`{view}`")
    print(f"  dropped view   : {view}")

print("✓ Step 1 complete — tables and views dropped")

# COMMAND ----------

# DBTITLE 1,Teardown Step 2 — Drop Schemas and Catalog
spark.sql(f"DROP SCHEMA IF EXISTS `{MY_CATALOG}`.`{DQ_SCHEMA}` CASCADE")
print(f"  dropped schema : {MY_CATALOG}.{DQ_SCHEMA}")

spark.sql(f"DROP SCHEMA IF EXISTS `{MY_CATALOG}`.`{CURATED_SCHEMA}` CASCADE")
print(f"  dropped schema : {MY_CATALOG}.{CURATED_SCHEMA}")

spark.sql(f"DROP CATALOG IF EXISTS `{MY_CATALOG}` CASCADE")
print(f"  dropped catalog: {MY_CATALOG}")

print("✓ Teardown complete")
