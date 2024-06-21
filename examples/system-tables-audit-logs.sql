-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Audit Logs
-- MAGIC -- https://docs.databricks.com/en/administration-guide/system-tables/audit-logs.html
-- MAGIC
-- MAGIC # Run
-- MAGIC - This notebook is entirelly sql; rec'd running against serverless sql warehouse for best experience.

-- COMMAND ----------

-- This query uses the information_schema to find out which users have permissions on a table.

SELECT DISTINCT(grantee) AS `ACCESSIBLE BY`
FROM system.information_schema.table_privileges
WHERE table_schema = 'faker' AND table_name = '<catalog>.faker.customer_churn_single'
  UNION
    SELECT table_owner
    FROM system.information_schema.tables
    WHERE table_schema = 'faker' AND table_name = '<catalog>.faker.customer_churn_single'
  UNION
    SELECT DISTINCT(grantee)
    FROM system.information_schema.schema_privileges
    WHERE schema_name = 'faker'


-- COMMAND ----------

-- Which users accessed a table within the last day?

SELECT
  user_identity.email as `User`,
  IFNULL(request_params.full_name_arg,
    request_params.name)
    AS `Table`,
    action_name AS `Type of Access`,
    event_time AS `Time of Access`
FROM system.access.audit
WHERE (request_params.full_name_arg = '<catalog>.faker.customer_churn_single'
  OR (request_params.name = 'customer_churn_single'
  AND request_params.schema_name = 'faker'))
  AND action_name
    IN ('createTable','getTable','deleteTable')
  AND event_date > now() - interval '1 day'
ORDER BY event_date DESC

-- COMMAND ----------

-- Which tables did a user access?

SELECT
        action_name as `EVENT`,
        event_time as `WHEN`,
        IFNULL(request_params.full_name_arg, 'Non-specific') AS `TABLE ACCESSED`,
        IFNULL(request_params.commandText,'GET table') AS `QUERY TEXT`
FROM system.access.audit
WHERE user_identity.email = '<email>'
        AND action_name IN ('createTable','commandSubmit','getTable','deleteTable')
        AND datediff(now(), event_date) < 1
        ORDER BY event_date DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ----

-- COMMAND ----------

SELECT *
FROM system.access.audit 
WHERE user_identity.email = "<email>" 
  and event_date > now() - interval '5 day'
  AND action_name NOT IN ('tokenLogin')
ORDER BY event_time DESC;

-- COMMAND ----------


