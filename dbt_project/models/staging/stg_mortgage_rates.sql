-- stg_mortgage_rates.sql
-- Cleans RBNZ mortgage rate data (B20, B21, B30 series)
-- Filters to relevant terms, casts types, adds rate category label

WITH source AS (
    SELECT * FROM {{ source('raw', 'MORTGAGE_RATES') }}
),

cleaned AS (
    SELECT
        TRY_CAST(DATE AS DATE)          AS rate_date,
        TRIM(TERM)                      AS term,
        TRY_CAST(RATE_PCT AS FLOAT)     AS rate_pct,
        SERIES                          AS series,

        -- label each series for readability
        CASE SERIES
            WHEN 'mortgage_special_rates'  THEN 'Special (Discounted)'
            WHEN 'mortgage_standard_rates' THEN 'Standard (Carded)'
            WHEN 'mortgage_weighted_avg'   THEN 'Weighted Average'
            ELSE SERIES
        END                             AS series_label

    FROM source
    WHERE DATE IS NOT NULL
    AND   RATE_PCT IS NOT NULL
    AND   TERM IS NOT NULL
    -- filter to most relevant terms for affordability analysis
    AND   TERM IN ('Floating', '6 months', '1 year', '18 months',
                   '2 years', '3 years', '4 years', '5 years')
)

SELECT * FROM cleaned