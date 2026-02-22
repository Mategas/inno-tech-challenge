-- This file contains SQL code to perform QA checks on the staging and dimension tables after they have been created and loaded
-- These checks are not being executed as part of the main transform.py script as output won't show, but can be run manually as needed to validate the data after running the staging and dimension scripts

-- Count rows
SELECT 'stg_breweries' AS table_name, COUNT(*) AS cnt FROM stg_breweries
UNION ALL
SELECT 'dim_type', COUNT(*) FROM dim_type
UNION ALL
SELECT 'dim_geo', COUNT(*) FROM dim_geo
UNION ALL
SELECT 'dim_brewery', COUNT(*) FROM dim_brewery; -- Should match stg_breweries count

-- Any breweries that failed to match with a type or geo?
SELECT
    SUM(CASE WHEN type_id IS NULL THEN 1 ELSE 0 END) AS missing_type,
    SUM(CASE WHEN geo_id IS NULL THEN 1 ELSE 0 END) AS missing_geo
FROM dim_brewery;