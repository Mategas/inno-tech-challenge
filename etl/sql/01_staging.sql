-- Here we create the staging table for breweries, which will be used as the source for the dimensional model

DROP TABLE IF EXISTS stg_breweries;

CREATE TABLE stg_breweries AS
SELECT
    id AS brewery_id,
    TRIM(name) AS name,
    LOWER(TRIM(brewery_type)) AS brewery_type,

    -- Geo fields (city-level dim)
    NULLIF(TRIM(city), '') AS city,
    NULLIF(TRIM(state_province), '') AS state_province,
    NULLIF(TRIM(country), '') AS country,

    -- We're skipping the state coulmn as its duplicated with state_province and deprecated in the API

    -- Brewery-level location attributes
    NULLIF(TRIM(postal_code), '') AS postal_code,
    CASE
        WHEN TRIM(latitude) IN ('', 'null') THEN NULL
        ELSE CAST(latitude AS REAL)
    END AS latitude,
    CASE
        WHEN TRIM(longitude) IN ('', 'null') THEN NULL
        ELSE CAST(longitude AS REAL)
    END AS longitude,
    NULLIF(TRIM(address_1), '') AS address_1,
    NULLIF(TRIM(address_2), '') AS address_2,
    NULLIF(TRIM(address_3), '') AS address_3,

    NULLIF(TRIM(phone), '') AS phone,
    NULLIF(TRIM(website_url), '') AS website_url,

    -- We're skipping the street column as its duplicated with address_1 and deprecated in the API

    ingested_at_utc
FROM raw_breweries
WHERE id IS NOT NULL;
