-- stg_building_consents.sql
-- Cleans raw building consent data from Stats NZ
-- Removes junk rows, casts types, renames columns

WITH source AS (
    SELECT * FROM {{ source('raw', 'BUILDING_CONSENTS') }}
),

cleaned AS (
    SELECT
        TRY_CAST(PERIOD AS INT)             AS consent_year,
        TRY_CAST(HOUSES AS INT)             AS houses,
        TRY_CAST(APARTMENTS AS INT)         AS apartments,
        TRY_CAST(RETIREMENT_UNITS AS INT)   AS retirement_units,
        TRY_CAST(TOWNHOUSES AS INT)         AS townhouses,
        TRY_CAST(ALL_DWELLINGS AS INT)      AS all_dwellings,
        TRY_CAST(FLOOR_AREA AS FLOAT)       AS floor_area_000sqm,
        TRY_CAST(VALUE AS FLOAT)            AS value_million_nzd
    FROM source
    -- remove junk rows — only keep rows where period is a valid year
    -- only keep annual totals, not monthly breakdowns
    WHERE TRY_CAST(PERIOD AS INT) IS NOT NULL
    AND   TRY_CAST(PERIOD AS INT) > 2000
    AND   PERIOD NOT LIKE '%Month%'
)

SELECT * FROM cleaned