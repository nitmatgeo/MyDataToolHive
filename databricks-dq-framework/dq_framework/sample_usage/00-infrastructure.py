# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Infrastructure Setup (run once per environment)
# MAGIC Create the Catalog, Schemas, and Volume.
# MAGIC Run this notebook **once** when setting up a new environment.
# MAGIC After this completes, proceed to `01-install.py` which installs the framework
# MAGIC and automatically copies the sample CSV files into the volume.

# COMMAND ----------

# DBTITLE 1,Initialize Catalog and Schema
MY_CATALOG      = "sampledatacatalog"   # ← change to your catalog
CURATED_SCHEMA  = "silver"              # ← schema that holds your curated tables
DQ_SCHEMA       = "dq"                  # ← schema where DQ framework tables will live
REPO_USER       = spark.sql("SELECT current_user()").first()[0]

SAMPLE_USAGE_PATH = f"/Workspace/Repos/{REPO_USER}/databricks-dq-framework/sample_usage"

print(f"Repo user        : {REPO_USER}")
print(f"Sample usage path: {SAMPLE_USAGE_PATH}")

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

# DBTITLE 1,Create Managed Catalog and Schemas
queries = [
    f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG} MANAGED LOCATION '{MANAGED_LOCATION}'",
    f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{CURATED_SCHEMA}",
    f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{DQ_SCHEMA}",
]

for q in queries:
    spark.sql(q)

print(f"✓ Catalog  : {MY_CATALOG}")
print(f"✓ Schema   : {MY_CATALOG}.{CURATED_SCHEMA}")
print(f"✓ Schema   : {MY_CATALOG}.{DQ_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Volume for Sample Data
# MAGIC The volume is the staging area for the 3 sample CSV files.
# MAGIC `01-install.py` will copy the files here automatically after the framework is installed.

# COMMAND ----------

# DBTITLE 1,Create Storage Volume for Sample Data
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{CURATED_SCHEMA}.sample_data")

print(f"✓ Volume created : {MY_CATALOG}.{CURATED_SCHEMA}.sample_data")
print(f"\nNext step: run 01-install.py")
print("  It will install the framework, copy the 3 sample CSVs from the")
print("  Repos path into this volume, and load them as Delta tables.")
