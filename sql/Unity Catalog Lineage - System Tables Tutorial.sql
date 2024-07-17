-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Notebook accompanying the System Tables, Lineage Deep Dive blog
-- MAGIC
-- MAGIC The data exploration section of the article walks through the cells below with narrative. 
-- MAGIC
-- MAGIC Please execute on a Serverless SQL Warehouse for the best experience.  Unity Catalog and system tables must be enabled, specifically the lineage system tables.
-- MAGIC
-- MAGIC First we will verify that the table lineage table has entries.  
-- COMMAND ----------
-- verify the timing of the latest entry in the system table
SELECT
  max(event_time)
FROM
  system.access.table_lineage
WHERE
  entity_run_id is null -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC All accesses to the billing usage table over the last 7 days
  -- COMMAND ----------
  -- select the last 7 days of accesses to the billing usage table
  -- note the different entity_types that might be accessing the data
  -- scroll right and see if there are any target sources where we have derivative data products being created
SELECT
  *
FROM
  system.access.table_lineage
WHERE
  source_table_full_name = 'system.billing.usage'
  AND datediff(now(), event_date) < 7 -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC
  -- MAGIC ### Setup
  -- MAGIC
  -- MAGIC We declare values to use throughout the rest of the notebook. 
  -- COMMAND ----------
  -- set up variables for data exploration
  -- change the variables to suit what you want to investigate
  DECLARE
  OR REPLACE VARIABLE catalog_val STRING;

DECLARE
OR REPLACE VARIABLE target_catalog_val STRING;

DECLARE
OR REPLACE VARIABLE schema_val STRING;

DECLARE
OR REPLACE VARIABLE table_val STRING;

DECLARE
OR REPLACE VARIABLE column_val STRING;

DECLARE
OR REPLACE VARIABLE email_val STRING;

DECLARE
OR REPLACE VARIABLE table_full_name_val STRING;

SET
  VARIABLE catalog_val = 'system';

SET
  VARIABLE target_catalog_val = 'pdavis';

SET
  VARIABLE schema_val = 'billing';

SET
  VARIABLE table_val = 'usage';

SET
  VARIABLE column_val = 'usage_quantity';

SET
  VARIABLE table_full_name_val = concat(catalog_val, '.', schema_val, '.', table_val);

SET
  VARIABLE email_val = 'demo@databricks.com';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Table level queries
-- COMMAND ----------
-- MAGIC %md
-- MAGIC
-- MAGIC Which users accessed a table and what Databricks interface did they use to access the table?
-- COMMAND ----------
-- Who accessed a single table: 
-- Which users accessed the table and what entity/interface did they access the table from?
SELECT
  mask(created_by) as created_by,
  entity_type,
  source_type,
  COUNT(distinct event_time) as access_count,
  MIN(event_date) as first_access_date,
  MAX(event_date) as last_access_date
FROM
  system.access.table_lineage
WHERE
  source_type is not NULL
  AND source_table_catalog = catalog_val
  AND source_table_schema = schema_val
  AND source_table_name = table_val
  AND datediff(now(), event_date) < 7
  AND entity_type IS NOT NULL
  AND source_type IS NOT NULL
GROUP BY
  ALL
ORDER BY
  ALL -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC Accesses of tables in a specific catalog and schema:  
  -- COMMAND ----------
  -- Show me counts of accesses for all tables within a particular catalog and schema
SELECT
  source_table_name,
  entity_type,
  created_by,
  source_type,
  COUNT(distinct event_time) as access_count,
  MIN(event_date) as first_access_date,
  MAX(event_date) as last_access_date
FROM
  system.access.table_lineage
WHERE
  source_table_catalog = catalog_val
  AND source_table_schema = schema_val
  AND datediff(now(), event_date) < 30
GROUP BY
  ALL
ORDER BY
  ALL -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC Which tables did a specific user access in the system catalog over the last 90 days? 
  -- MAGIC
  -- COMMAND ----------
SELECT
  source_table_catalog,
  source_table_schema,
  source_table_name,
  entity_type,
  source_type,
  mask(created_by) as created_by,
  COUNT(distinct event_time) as access_count,
  MIN(event_date) as first_access_date,
  MAX(event_date) as last_access_date
FROM
  system.access.table_lineage
WHERE
  created_by = email_val
  AND datediff(now(), event_date) < 90
  and entity_type is not NULL
  and source_table_catalog = 'system'
GROUP BY
  ALL
ORDER BY
  ALL -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC The most popular system table objects
  -- COMMAND ----------
  -- Most popular system table object as measured by a simple count of accesses over the last 7 days.  
  -- Modify as needed for any catalog or schema.
SELECT
  source_table_full_name,
  count(*) as lineage_total
FROM
  system.access.table_lineage
WHERE
  datediff(now(), event_date) < 7
  AND source_table_catalog = 'system'
GROUP BY
  ALL
ORDER by
  lineage_total DESC
LIMIT
  5 -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC The least popular tables over the last 90 days
  -- COMMAND ----------
SELECT
  source_table_full_name,
  count(*) as lineage_total
FROM
  system.access.table_lineage
WHERE
  datediff(now(), event_date) < 7
  AND source_table_catalog = 'system'
GROUP BY
  ALL
ORDER by
  lineage_total DESC
LIMIT
  5 -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC For a single object, what are the immediate upstream and downstream objects?
  -- COMMAND ----------
  with downstream AS (
    select
      distinct target_table_catalog as table_catalog,
      target_table_schema as table_schema,
      target_table_name as table_name,
      'downstream' as direction,
      CASE
        WHEN tbl.table_catalog is null then 'no'
        else 'yes'
      end as current
    from
      system.access.table_lineage tl
      left join system.information_schema.tables tbl on tl.target_table_full_name = concat(
        tbl.table_catalog,
        '.',
        tbl.table_schema,
        '.',
        tbl.table_name
      )
    where
      source_table_full_name = table_full_name_val
      AND target_table_full_name is not null
    order by
      current desc,
      table_catalog,
      table_schema,
      table_name
  ),
  upstream AS (
    select
      distinct source_table_catalog as table_catalog,
      source_table_schema as table_schema,
      source_table_name as table_name,
      'upstream' as direction,
      CASE
        WHEN tbl.table_catalog is null then 'no'
        else 'yes'
      end as current
    from
      system.access.table_lineage tl
      left join system.information_schema.tables tbl on tl.source_table_full_name = concat(
        tbl.table_catalog,
        '.',
        tbl.table_schema,
        '.',
        tbl.table_name
      )
    where
      target_table_full_name = table_full_name_val
      AND source_table_full_name is not null
    order by
      current desc,
      table_catalog,
      table_schema,
      table_name
  )
select
  *
from
  upstream
UNION
ALL
select
  *
from
  downstream -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC ### Column level queries
  -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC The most referenced columns and their tables in a catalog over the last 90 days
  -- COMMAND ----------
SELECT
  source_column_name,
  source_table_full_name,
  COUNT(*) AS frequency
FROM
  system.access.column_lineage
WHERE
  1 = 1
  AND source_type <> 'PATH'
  AND datediff(now(), event_date) < 90
  AND source_table_catalog = catalog_val
GROUP BY
  source_column_name,
  source_table_full_name
ORDER BY
  frequency DESC
LIMIT
  10 -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC Column reads and target downstream columns/tables from a single column over the last 90 days
  -- COMMAND ----------
SELECT
  target_column_name,
  target_table_full_name,
  COUNT(*) AS frequency
FROM
  system.access.column_lineage
WHERE
  1 = 1
  AND source_column_name = column_val
  AND source_table_full_name = table_full_name_val
  AND datediff(now(), event_date) < 90
  AND target_table_full_name IS NOT NULL
  AND target_table_catalog = target_catalog_val
GROUP BY
  target_column_name,
  target_table_full_name
ORDER BY
  frequency DESC
LIMIT
  10 -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC Accesses of the column over the last 90 days
  -- COMMAND ----------
SELECT
  event_date,
  COUNT(*) AS frequency
FROM
  system.access.column_lineage
WHERE
  1 = 1
  AND source_column_name = column_val
  AND source_table_full_name = table_full_name_val
  AND datediff(now(), event_date) < 90
GROUP BY
  ALL -- COMMAND ----------
  -- MAGIC %md
  -- MAGIC Who has read from a column over the last 7 days
  -- COMMAND ----------
SELECT
  mask(created_by) as created_by,
  entity_type,
  source_type,
  COUNT(distinct event_time) as access_count,
  MIN(event_date) as first_access_date,
  MAX(event_date) as last_access_date
FROM
  system.access.column_lineage
WHERE
  source_table_catalog = catalog_val
  AND source_table_schema = schema_val
  AND source_table_name = table_val
  AND source_column_name = column_val
  AND datediff(now(), event_date) < 90
  AND entity_type is not NULL
  AND source_type IS NOT NULL
GROUP BY
  ALL
ORDER BY
  last_access_date desc
LIMIT
  10
