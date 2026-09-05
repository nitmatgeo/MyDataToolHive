# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Infrastructure Setup (run once per environment)
# MAGIC Create the Catalog and ETL Monitoring Schema.
# MAGIC Run this notebook **once** when setting up a new environment.
# MAGIC After this completes, proceed to `01-install.py` which installs the framework
# MAGIC and initialises all monitoring tables and views.

# COMMAND ----------

# DBTITLE 1,Define Catalog and Schema Variables
MY_CATALOG  = "sampledatacatalog"   # ← change to your catalog
ETL_SCHEMA  = "etl"                 # ← schema where ETL monitoring tables will live

REPO_USER = spark.sql("SELECT current_user()").first()[0]

print(f"Repo user  : {REPO_USER}")
print(f"Catalog    : {MY_CATALOG}")
print(f"ETL schema : {ETL_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Resolve Managed Storage Root URL for Workspace
workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.orgId")
query = f"""
    SELECT url
    FROM system.information_schema.external_locations
    WHERE external_location_owner LIKE '%{workspace_id}%' AND url LIKE '%{workspace_id}%'
"""
results = spark.sql(query).collect()

if not results:
    raise Exception(
        f"Could not resolve managed storage root for workspace '{workspace_id}'. "
        "Ensure Unity Catalog is enabled and the workspace default external location exists "
        "in system.information_schema.external_locations."
    )

managed_root = results[0]["url"]
MANAGED_LOCATION = f"{managed_root}/{MY_CATALOG}"
print(f"Managed location : {MANAGED_LOCATION}")

# COMMAND ----------

# DBTITLE 1,Create Managed Catalog and ETL Schema
queries = [
    f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG} MANAGED LOCATION '{MANAGED_LOCATION}'",
    f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{ETL_SCHEMA}",
]

for q in queries:
    spark.sql(q)

print(f"✓ Catalog    : {MY_CATALOG}")
print(f"✓ ETL schema : {MY_CATALOG}.{ETL_SCHEMA}")
print(f"\nNext step: run 01-install.py")
print("  It will install the framework and create all monitoring tables and views.")
