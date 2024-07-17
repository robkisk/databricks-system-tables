-- Who last updated the gold tables and when? 
SELECT
    table_name,
    last_altered_by,
    last_altered
FROM
    system.information_schema.tables
WHERE
    table_schema = "churn_gold"
ORDER BY
    1,
    3 DESC;

-- What tables are in the sales catalog?
SELECT
    table_name
FROM
    system.information_schema.tables
WHERE
    table_catalog = "sales"
    AND table_schema != "information_schema";

-- Who owns this gold table? 
SELECT
    table_owner
FROM
    system.information_schema.tables
WHERE
    table_catalog = "retail_prod"
    AND table_schema = "churn_gold"
    AND table_name = "churn_features";

-- Who has access to this table?
SELECT
    grantee,
    table_name,
    privilege_type
FROM
    system.information_schema.table_privileges
WHERE
    table_name = "login_data_silver";

/* -------------------------- Common Audit Queries -------------------------- */
-- What has this user accessed in the last 24 hours?
SELECT
    request_params.table_full_name
FROM
    system.access.audit
WHERE
    user_identity.email = "<user_email>"
    AND service_name = "unityCatalog"
    AND action_name = "generateTemporaryTableCredential"
    AND datediff(now(), created_at) < 1;

-- Who accesses this table the most?
SELECT
    user_identity.email,
    count(*)
FROM
    system.access.audit
WHERE
    request_params.table_full_name = "main.uc_deep_dive.login_data_silver"
    AND service_name = "unityCatalog"
    AND action_name = "generateTemporaryTableCredential"
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT
    1;

-- Who deleted this table?
SELECT
    user_identity.email
FROM
    system.access.audit
WHERE
    request_params.full_name_arg = "main.uc_deep_dive.login_data_silver"
    AND service_name = "unityCatalog"
    AND action_name = "deleteTable";

-- What tables does this user access most frequently?
SELECT
    request_params.table_full_name,
    count(*)
FROM
    system.access.audit
WHERE
    user_identity.email = "<email>"
    AND service_name = "unityCatalog"
    AND action_name = "generateTemporaryTableCredential"
GROUP BY
    1
ORDER BY
    2 DESC
LIMIT
    1;

/* ------------------------- Common Billing Queries ------------------------- */
-- Which 10 users consumed the most DBUs? 
SELECT
    tags.creator as `User`,
    sum(dbus) as `DBUs`
FROM
    system.billing.usage
GROUP BY
    tags.creator
ORDER BY
    `DBUs` DESC
LIMIT
    10;

-- Which Jobs consumed the most DBUs?
SELECT
    tags.JobId as `Job ID`,
    sum(dbus) as `DBUs`
FROM
    system.billing.usage
GROUP BY
    `Job ID`;

-- What is the daily trend in DBU consumption?
SELECT
    date(created_on) as `Date`,
    sum(dbus) as `DBUs Consumed`
FROM
    system.billing.usage
GROUP BY
    date(created_on)
ORDER BY
    date(created_on) ASC;

-- How many DBUs of each SKU have been used so far this month?
SELECT
    sku as `SKU`,
    sum(dbus) as `DBUs`
FROM
    system.billing.usage
WHERE
    month(created_on) = month(CURRENT_DATE)
GROUP BY
    sku
ORDER BY
    `DBUs` DESC;
