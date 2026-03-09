-- stg_hud_rental.sql
-- Cleans HUD Rental Price Index data
-- Standardises region names, ensures proper date format, removes nulls

WITH source AS (
    SELECT * FROM {{ source('raw', 'HUD_RENTAL_INDEX') }}
),

cleaned AS (
    SELECT
        -- standardise region names
        TRIM(UPPER(REGION))                             AS region,
        TRY_CAST(PERIOD AS DATE)                        AS period,
        TRY_CAST(RENTAL_PRICE_INDEX AS FLOAT)           AS rental_price_index,
        TRY_CAST(ANNUAL_CHANGE AS FLOAT)                AS annual_change_pct

    FROM source
    WHERE REGION IS NOT NULL
    AND   PERIOD IS NOT NULL
    AND   RENTAL_PRICE_INDEX IS NOT NULL
)

SELECT * FROM cleaned