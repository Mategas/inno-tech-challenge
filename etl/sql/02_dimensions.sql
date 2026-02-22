-- This file contains the SQL code to create the dimension tables for the brewery data.
-- We have a type dimension, a city-level geo dimension, and a brewery dimension that links to both the type and geo dimensions and contains brewery-level attributes.

-- =========================
-- DIM_TYPE
-- =========================
DROP TABLE IF EXISTS dim_type;

CREATE TABLE dim_type AS
WITH types AS (
    SELECT DISTINCT brewery_type
    FROM stg_breweries
    WHERE brewery_type IS NOT NULL AND brewery_type <> ''
)
SELECT
    ROW_NUMBER() OVER (ORDER BY brewery_type) AS type_id,
    brewery_type AS type,
    (SELECT MAX(ingested_at_utc) FROM stg_breweries) AS ingested_at_utc
FROM types;

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_type_type ON dim_type(type);


-- =========================
-- DIM_GEO (city-level)
-- =========================
DROP TABLE IF EXISTS dim_geo;

CREATE TABLE dim_geo AS
WITH geos AS (
    SELECT DISTINCT
        city,
        state_province,
        country
    FROM stg_breweries
    WHERE country IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            country,
            state_province,
            city
    ) AS geo_id,
    city,
    state_province,
    country,
    (SELECT MAX(ingested_at_utc) FROM stg_breweries) AS ingested_at_utc
FROM geos;

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_geo ON dim_geo(country, state_province, city);


-- =========================
-- DIM_BREWERY (Pseudo fact table with brewery-level attributes + FKs to city-level dim and type dim)
-- =========================
DROP TABLE IF EXISTS dim_brewery;

CREATE TABLE dim_brewery AS
SELECT
    s.brewery_id,
    s.name,
    t.type_id,
    g.geo_id,
    s.postal_code,
    s.latitude,
    s.longitude,
    s.address_1,
    s.address_2,
    s.address_3,
    s.phone,
    s.website_url,
    s.ingested_at_utc
FROM stg_breweries s
LEFT JOIN dim_type t
    ON t.type = s.brewery_type
LEFT JOIN dim_geo g
    ON g.country = s.country
   AND g.state_province = s.state_province
   AND g.city = s.city;

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_brewery_id ON dim_brewery(brewery_id);
CREATE INDEX IF NOT EXISTS ix_dim_brewery_type_id ON dim_brewery(type_id);
CREATE INDEX IF NOT EXISTS ix_dim_brewery_geo_id ON dim_brewery(geo_id);