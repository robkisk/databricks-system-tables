# Databricks notebook source
# MAGIC %md **Note:** This notebook was developed and tested on a cluster with 14.3 LTS Runtime with Shared Access Mode.

# COMMAND ----------

# MAGIC %md ## Introduction
# MAGIC The best way to track model servings costs in Databricks is through the [billable usage system table](https://docs.databricks.com/en/administration-guide/system-tables/billing.html). Once enabled, the table automatically populates with the latest usage in your Databricks account. No matter which of the model serving methods you choose, your costs will appear in the _system.billing.usage_ table with column _sku_name_ as either:
# MAGIC
# MAGIC **\<tier\>**\_SERVERLESS\_REAL\_TIME\_INFERENCE\_LAUNCH\_**\<region\>**
# MAGIC
# MAGIC which includes all DBUs accrued when an endpoint starts after scaling to zero. All other model serving costs are grouped under:
# MAGIC
# MAGIC **\<tier\>**\_SERVERLESS\_REAL\_TIME\_INFERENCE\_**\<region\>**
# MAGIC
# MAGIC where tier corresponds to your Databricks platform tier and region corresponds to the cloud region of your Databricks deployment.
# MAGIC
# MAGIC **Note**: The _system.billing.usage_ table contains usage for all workspaces in your Databricks account.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   system.billing.usage
# MAGIC WHERE
# MAGIC   sku_name LIKE '%SERVERLESS_REAL_TIME_INFERENCE%'
# MAGIC ORDER BY usage_start_time DESC
# MAGIC LIMIT
# MAGIC   5

# COMMAND ----------

# MAGIC %md ##Querying the Usage Table
# MAGIC
# MAGIC You can easily query the _system.billing.usage_ table to aggregate all DBUs (Databricks Units) associated with Databricks model serving. Here is an example query that aggregates model serving DBUs per day for the last 30 days:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   usage_date,
# MAGIC   SUM(usage_quantity) AS dbus
# MAGIC FROM
# MAGIC   system.billing.usage
# MAGIC WHERE
# MAGIC   sku_name LIKE '%SERVERLESS_REAL_TIME_INFERENCE%'
# MAGIC GROUP BY(usage_date)
# MAGIC ORDER BY
# MAGIC   usage_date DESC
# MAGIC LIMIT
# MAGIC   30

# COMMAND ----------

# MAGIC %md
# MAGIC If we want to modify the above query to instead track dollars spent using Databricks list prices, we can do that using another system table available through _system.billing.list_prices_.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   a.usage_date,
# MAGIC   SUM(a.usage_quantity * b.pricing ["default"]) AS dollars
# MAGIC FROM
# MAGIC   system.billing.usage a
# MAGIC   LEFT JOIN system.billing.list_prices b on a.sku_name = b.sku_name
# MAGIC WHERE
# MAGIC   a.sku_name LIKE '%SERVERLESS_REAL_TIME_INFERENCE%'
# MAGIC   AND b.pricing ["default"] IS NOT NULL
# MAGIC GROUP BY
# MAGIC   a.usage_date
# MAGIC ORDER BY
# MAGIC   a.usage_date DESC
# MAGIC LIMIT
# MAGIC   30

# COMMAND ----------

# MAGIC %md ## Retrieve Endpoint Names using the Python SDK
# MAGIC
# MAGIC The _system.billing.usage_ table contains the endpointId but does not include all useful information like endpoint name. To match endpoint names to endpoint IDs, you can use the Databricks SDK for Python([AWS](https://docs.databricks.com/en/dev-tools/sdk-python.html)/[Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/sdk-python)/[GCP](https://docs.gcp.databricks.com/en/dev-tools/sdk-python.html?_ga=2.168959777.1699285005.1714401526-1751985763.1704316196)) to query the data from the [Databricks REST API](https://docs.databricks.com/api/workspace/introduction).
# MAGIC
# MAGIC **Note**: The REST API will return all endpoints within the current workspace.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

# The SDK should pick up your credentials from your notebook.
# But if you have issues see the Authentication docs for
# the Python SDK: https://databricks-sdk-py.readthedocs.io/en/latest/authentication.html
w = WorkspaceClient()

# Iterate through all endpoints and store the results in a list.
endpoints = []

for endpoint in w.serving_endpoints.list():
    endpoints.append(endpoint.as_dict())

print("There are {} endpoints in your workspace!".format(len(endpoints)))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's convert this data to a Spark DataFrame so we can combine it with our usage data.

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import (
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Define the schema for the DataFrame
schema = StructType(
    [
        StructField("config", StringType(), nullable=True),
        StructField("creation_timestamp", StringType(), nullable=True),
        StructField("creator", StringType(), nullable=True),
        StructField("id", StringType(), nullable=True),
        StructField("last_updated_timestap", TimestampType(), nullable=True),
        StructField("name", StringType(), nullable=False),
        StructField("state", MapType(StringType(), StringType()), nullable=True),
        StructField("tags", StringType(), nullable=True),
        StructField("task", StringType(), nullable=True),
    ]
)

# Set the desired column order.
col_order = [
    "name",
    "id",
    "creator",
    "tags",
    "task",
    "state_update",
    "state_ready",
    "creation_timestamp",
    "last_updated_timestap",
]

# Break out the map column as separate columns.
endpoints_df = (
    spark.createDataFrame(endpoints, schema)
    .withColumn("state_update", col("state").getItem("config_update"))
    .withColumn("state_ready", col("state").getItem("ready"))
    .drop(col("state"))
    .select(col_order)
)

endpoints_df.createOrReplaceTempView("endpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Now with the additional data from the REST API we can do more complex queries, like returning the name of the 5 most expensive endpoints:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE
# MAGIC OR REPLACE TEMPORARY VIEW endpoint_usage AS
# MAGIC SELECT
# MAGIC   custom_tags ['EndpointId'] AS endpoint_id,
# MAGIC   usage_quantity,
# MAGIC   usage_quantity * pricing ["default"] AS dollars,
# MAGIC   usage_date,
# MAGIC   a.account_id,
# MAGIC   workspace_id
# MAGIC FROM
# MAGIC   system.billing.usage a
# MAGIC   LEFT JOIN system.billing.list_prices b ON a.sku_name = b.sku_name
# MAGIC WHERE
# MAGIC   a.sku_name LIKE '%SERVERLESS_REAL_TIME_INFERENCE%'
# MAGIC   AND a.custom_tags ['EndpointId'] IS NOT NULL;
# MAGIC SELECT
# MAGIC   name,
# MAGIC   SUM(dollars) AS cost
# MAGIC FROM
# MAGIC   endpoint_usage
# MAGIC   INNER JOIN field_demos.sewi.endpoints ON endpoint_id = id
# MAGIC GROUP BY
# MAGIC   name
# MAGIC ORDER BY
# MAGIC   cost DESC
# MAGIC LIMIT
# MAGIC   5

# COMMAND ----------

# MAGIC %md ## Customize Cost Tracking with Tags
# MAGIC Aggregated costs may be sufficient for simple use cases, but as the number of endpoints grows it is desirable to break out costs based on use case, business unit, or other custom identifiers. Optional key/value tags can be applied to custom models endpoints. All custom tags applied to Databricks Model Serving endpoints propagate to the _system.billing.usage_ table under the _custom_tags_ column and can be used to aggregate and visualize costs. Databricks recommends adding descriptive tags to each endpoint for precise cost tracking.
# MAGIC
# MAGIC For example, you can apply a "Cost Center" to each endpoint so you can attribute costs to specific business units.
# MAGIC
# MAGIC In the example below, costs are broken out by the ServingType tag to show aggregate costs by CPU/GPU/pay-per-token/feature serving.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   custom_tags ["ServingType"] AS value,
# MAGIC   SUM(usage_quantity) AS DBUs
# MAGIC FROM
# MAGIC   system.billing.usage
# MAGIC WHERE
# MAGIC   sku_name LIKE '%SERVERLESS_REAL_TIME_INFERENCE%'
# MAGIC   AND custom_tags ["ServingType"] IS NOT NULL
# MAGIC   AND usage_date > DATE_SUB(CURRENT_DATE(), 30)
# MAGIC GROUP BY
# MAGIC   custom_tags ["ServingType"]
# MAGIC ORDER BY
# MAGIC   DBUs DESC

# COMMAND ----------

# MAGIC %md ##Creating a Budget Notification
# MAGIC
# MAGIC Using these tables you can easily create email or Slack alerts when your spend exceeds a set budget using Databricks Alerts ([AWS](https://docs.databricks.com/en/sql/user/alerts/index.html)/[Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/user/alerts/)/[GCP](https://docs.gcp.databricks.com/en/sql/user/alerts/index.html?_ga=2.139023667.1699285005.1714401526-1751985763.1704316196)). First, create a query that returns spend over your time range of interest.
# MAGIC
# MAGIC Here is a query that returns costs associated with a specific endpoint over the course of the current month:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   name,
# MAGIC   SUM(endpoint_usage.dollars) AS cost
# MAGIC FROM
# MAGIC   endpoint_usage
# MAGIC   INNER JOIN field_demos.sewi.endpoints ON endpoint_id = id
# MAGIC WHERE
# MAGIC   MONTH(endpoint_usage.usage_date) = MONTH(NOW())
# MAGIC   AND YEAR(endpoint_usage.usage_date) = YEAR(NOW())
# MAGIC -- Replace with the name of the endpoint you are interested in.
# MAGIC   AND name = 'ya_dbrx'
# MAGIC GROUP BY
# MAGIC   name

# COMMAND ----------

# MAGIC %md
# MAGIC Next, save this query in your workspace ([AWS](https://docs.databricks.com/en/sql/user/sql-editor/index.html#save-queries)/[Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/user/sql-editor/#save-queries)/[GCP](https://docs.gcp.databricks.com/en/sql/user/sql-editor/index.html#save-queries)).

# COMMAND ----------

query_template = """
CREATE OR REPLACE TEMPORARY VIEW endpoint_usage AS 
  SELECT
    custom_tags['EndpointId'] AS endpoint_id, usage_quantity, usage_quantity*pricing["default"] AS dollars, usage_date, a.account_id, workspace_id
  FROM
      system.billing.usage a left join system.billing.list_prices b on a.sku_name = b.sku_name
  WHERE
    a.sku_name LIKE '%SERVERLESS_REAL_TIME_INFERENCE%'
    AND a.custom_tags ['EndpointId'] IS NOT NULL;

SELECT name, SUM(endpoint_usage.dollars) AS cost
 FROM endpoint_usage 
 INNER JOIN field_demos.sewi.endpoints ON endpoint_id = id
 WHERE MONTH(endpoint_usage.usage_date) = MONTH(NOW())
 AND YEAR(endpoint_usage.usage_date) = YEAR(NOW())
 -- Replace with the name of the endpoint you are interested in.
 AND name = '{endpoint_name}'
 GROUP BY name
 """

# COMMAND ----------

# Replace endpoint_name with your endpoint of interest
endpoint_name = "ya_dbrx"
query_text = query_template.format(endpoint_name=endpoint_name)

query = w.queries.create(
    name="model_serving_budget",
    description="Return the amount spent on an endpoint over a given month",
    query=query_text,
)

# COMMAND ----------

# MAGIC %md
# MAGIC Then create an "Alert" based off the query. Alerts are scheduled queries than can trigger email, Slack, or webhook notifications.

# COMMAND ----------

from databricks.sdk.service.sql import AlertOptions

# Replace with your desired cost threshold, in dollars.
alert_cost_thresh = 1000

options = AlertOptions(column="cost", op=">", value=alert_cost_thresh)

alert = w.alerts.create(name="sdk_test_alert", query_id=query.id, options=options)

# Retrieve url of the workspace
base_url = spark.conf.get("spark.databricks.workspaceUrl", None)
alert_url = "https://" + base_url + "/sql/alerts/" + alert.id

print(
    "New alert successfully created! Navigate to {} to take a look or modify it.".format(
        alert_url
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, select "Add schedule" to configure the frequency to run the budget check. Select the "Destinations" tab ([AWS](https://docs.databricks.com/en/administration-guide/workspace-settings/notification-destinations.html)/[Azure](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/workspace-settings/notification-destinations)/[GCP](https://docs.gcp.databricks.com/en/administration-guide/workspace-settings/notification-destinations.html)) to configure email or Slack messages to be sent when the budget is exceeded.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Save Query](/files/sewi/Add_Schedule.png)
