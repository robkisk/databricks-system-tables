-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Run
-- MAGIC - This notebook is entirelly sql; rec'd running against serverless sql warehouse for best experience.

-- COMMAND ----------

-- Who has access to this volume?
SELECT grantee, volume_name, privilege_type
FROM system.information_schema.volume_privileges
WHERE volume_name = "data_science_volume";

-- COMMAND ----------

-- Dashboard Name: DBSQL Governance Dashboard
-- Report Name: Volume Types by Created Mont
-- Query Name: dbsql-volume-types
SELECT
    volume_catalog,
    volume_schema,
    volume_type,
    DATE_TRUNC('month', created) AS month_created,
    COUNT(*) AS volume_count
FROM
    system.information_schema.volumes
WHERE
    volume_schema NOT IN ('information_schema')
    and volume_catalog like '%<string>%'
GROUP BY
    volume_catalog,
    volume_schema,
    DATE_TRUNC('month', created),
    volume_type
ORDER BY
    month_created ASC,
    volume_count DESC

-- COMMAND ----------

-- Is this volume used to create any tables?
SELECT target_table_full_name 
FROM  system.lineage.table_lineage 
WHERE 
source_type = "PATH" 
AND target_type = "TABLE" 
AND source_path LIKE "%/Volumes/<catalog>/data_science/data_science_volume%";

-- COMMAND ----------

-- Who accessed this volume in the past 7 days?
SELECT user_identity.email, request_params.operation 
FROM system.access.audit 
WHERE event_date >= current_date() - INTERVAL 7 DAYS 
AND action_name LIKE "%generateTemporaryVolumeCredential%" 
AND request_params.volume_full_name = "<catalog>.data_science.data_science_volume";

-- COMMAND ----------

-- Who has last downloaded this particular file?
SELECT user_identity.email, max(event_time) as last_access
FROM system.access.audit 
WHERE service_name LIKE "filesystem" 
AND action_name = "filesGet" 
AND request_params.path LIKE "/Volumes/<catalog>/data_science/data_science_volume/wind_farm/windfarm_data.csv"
GROUP BY 1
ORDER BY last_access DESC;

-- COMMAND ----------


