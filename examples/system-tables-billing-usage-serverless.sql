-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Billing
-- MAGIC - https://docs.databricks.com/en/admin/system-tables/billing.html
-- MAGIC - serverless specific: https://docs.databricks.com/en/admin/system-tables/serverless-billing.html
-- MAGIC
-- MAGIC # Run
-- MAGIC - This notebook is entirelly sql; rec'd running against serverless sql warehouse for best experience.
-- MAGIC

-- COMMAND ----------

select * 
from system.billing.usage 
where sku_name like '%SERVERLESS%' 
  and billing_origin_product IN ("NOTEBOOKS", "JOBS")
  and identity_metadata.run_as = "<email>"
ORDER BY usage_date DESC

-- COMMAND ----------

-- Report on DBUs consumed by a particular user
-- https://docs.databricks.com/en/admin/system-tables/serverless-billing.html#report-on-dbus-consumed-by-a-particular-user

SELECT
  usage_metadata.job_id,
  usage_metadata.notebook_id,
  SUM(usage_quantity) as total_dbu
FROM
  system.billing.usage
WHERE
  identity_metadata.run_as = '<email>'
  and billing_origin_product in ('JOBS','INTERACTIVE')
  and product_features.is_serverless -- SERVERLESS
  and usage_unit = 'DBU'
  and usage_date >= DATEADD(day, -30, current_date)
GROUP BY
  1,2
ORDER BY
  total_dbu DESC


-- COMMAND ----------


