# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks System Tables
# MAGIC * Now in Public Preview - https://docs.databricks.com/administration-guide/system-tables/index.html
# MAGIC * In the first cell below, change `metastore_id` to point to YOUR metastore_id
# MAGIC * Then run each cell below as a user who is an Account Admin to enable the System Table schemas.

# COMMAND ----------

# DBTITLE 1,Step 1 - Set YOUR UC Metastore ID here (get it in the Account Console, Data tab)
import requests
# change to YOUR metastore id
# find it on your Account Console > Data page at the end of the "Path" value
# for example: "s3://databricks-workspace-stack-42fd5-metastore-bucket/c1bb16b6-7770-4687-8d5e-e5ec3148d13c"
# or on Azure: "abfss://uc-metastore-root@thordemoprod.dfs.core.windows.net/72b3b15d-7176-45e6-a0df-4a478cf32f1c"
metastore_id = "40dc2041-0b89-484b-a4bd-8cd49d95c1bd"

# COMMAND ----------

# DBTITLE 1,Step 2 - Check the initial status of your system table schemas
host = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
headers = {"Authorization": "Bearer "+dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}

r = requests.get(f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas", headers=headers).json()

print(r)
# you should see only the information_schema schema in ENABLE_COMPLETED status
# just enable the access and billing schemas (the access schema now contains the lineage tables)
# note that the lineage and operational_data schemas are deprecated so don't enable them -- the sample output below is from an internal account that had early system table previews enabled.

# COMMAND ----------

# DBTITLE 1,Step 3a - Enable the Access schema
schema_name = 'access'
host = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
headers = {"Authorization": "Bearer "+dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}
r = requests.put(f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}", headers=headers).json()

print(r)

# note that the access schema contains the audit, column_lineage, and table_lineage tables

# COMMAND ----------

# DBTITLE 1,Step 3b - Enable the Billing schema
schema_name = 'billing'
host = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
headers = {"Authorization": "Bearer "+dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}
r = requests.put(f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/{schema_name}", headers=headers).json()

print(r)

# COMMAND ----------

# DBTITLE 1,Step 4 - Show new status of system table schemas
host = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
headers = {"Authorization": "Bearer "+dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}

r = requests.get(f"{host}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas", headers=headers).json()

print(r)
# you should see only the access, billing, and information_schema schemas in ENABLE_COMPLETED status
# visit the Data page from the side menu and open the System schema from the catalog tree. You may need to refresh the page if it was already open prior to running this notebook.
# notice that these system tables are a special kind of MANAGED table of sub-type TABLE_DELTASHARING since the data is provided to you via Delta Sharing
