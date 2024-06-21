-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Run
-- MAGIC - This notebook is entirelly sql; rec'd running against serverless sql warehouse for best experience.

-- COMMAND ----------

select * from system.information_schema.table_privileges where table_name = "<catalog>.data_eng.customer_qna" limit 10;

-- COMMAND ----------

-- Who can access this table
SELECT DISTINCT(grantee) AS `ACCESSIBLE BY`
FROM system.information_schema.table_privileges
WHERE table_schema = "data_eng" AND table_name = "customer_qna"
  UNION
    SELECT table_owner
    FROM system.information_schema.tables
    WHERE table_schema = "data_eng" AND table_name = "customer_qna"
  UNION
    SELECT DISTINCT(grantee)
    FROM system.information_schema.schema_privileges
    WHERE schema_name = "data_eng";

-- COMMAND ----------

-- Tables by Format
SELECT
    table_catalog AS cat_name,
    table_schema AS sch_name,
    data_source_format AS table_format,
    COUNT(*) AS table_count
FROM
    system.information_schema.tables
WHERE
    table_type != 'VIEW'
    AND table_schema NOT IN ('information_schema')
    AND table_catalog like '%<catalog>%'
GROUP BY
    cat_name,
    sch_name,
    table_format 
ORDER BY
    table_count DESC

-- COMMAND ----------


