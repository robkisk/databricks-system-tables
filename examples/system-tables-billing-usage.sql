-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Billing
-- MAGIC - https://docs.databricks.com/en/admin/system-tables/billing.html
-- MAGIC
-- MAGIC # Run
-- MAGIC - This notebook is entirelly sql; rec'd running against serverless sql warehouse for best experience.

-- COMMAND ----------

-- all job details
SELECT sku_name
  ,usage_start_time
  ,usage_end_time
  ,usage_date
  ,usage_quantity
  ,usage_metadata
  ,billing_origin_product
  ,product_features
  ,usage_type
  ,ingestion_date
  ,record_type
FROM
  system.billing.usage
WHERE
  identity_metadata.run_as = '<email>'
  and billing_origin_product in ('JOBS')
  and usage_metadata.job_id = "1048108608772559"
ORDER BY usage_start_time DESC

-- COMMAND ----------

-- DBU by job details
SELECT 
  usage_date
  ,sku_name
  ,SUM(usage_quantity) as total_dbu
FROM
  system.billing.usage
WHERE
  identity_metadata.run_as = '<email>'
  and billing_origin_product in ('JOBS')
  and usage_metadata.job_id = "1048108608772559"
GROUP BY
  1,2
ORDER BY usage_date DESC

-- COMMAND ----------

-- DBUs by user

SELECT
  sku_name,
  usage_metadata.job_id,
  usage_metadata.notebook_id,
  SUM(usage_quantity) as total_dbu
FROM
  system.billing.usage
WHERE
  identity_metadata.run_as = '<email>'
  and billing_origin_product in ('JOBS','INTERACTIVE')
  and product_features.is_serverless -- for serverless only
  and usage_unit = 'DBU'
  and usage_date >= DATEADD(day, -30, current_date)
GROUP BY
  1,2,3
ORDER BY
  total_dbu DESC


-- COMMAND ----------

-- DBUs by cluster
SELECT
  usage_date,
  sku_name,
  SUM(usage_quantity) as total_dbu
FROM 
  system.billing.usage
WHERE 
  usage_metadata.cluster_id = '0220-165449-kbe0uvgf'
  and usage_date >= DATEADD(day, -30, current_date)
  -- AND usage_unit = 'DBUs'
GROUP BY
  1,2
ORDER BY 
  usage_date DESC;

-- COMMAND ----------


