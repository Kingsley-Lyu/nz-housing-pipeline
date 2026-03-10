-- mart_affordability.sql
-- NZ Housing Affordability Mart
-- Joins rental index, mortgage rates, and building consents
-- to produce a unified affordability view by region and month

WITH rental AS (
    SELECT
        region,
        period,
        rental_price_index,
        annual_change_pct
    FROM {{ ref('stg_hud_rental') }}
),

-- get 1 year special mortgage rate per month
-- this is the most common rate NZ borrowers use
mortgage AS (
    SELECT
        rate_date,
        rate_pct AS one_year_special_rate
    FROM {{ ref('stg_mortgage_rates') }}
    WHERE term   = '1 year'
    AND   series = 'mortgage_special_rates'
),

-- get latest available consent year for each pipeline run
-- we use yearly consents as a proxy for housing supply pressure
consents AS (
    SELECT
        consent_year,
        all_dwellings,
        houses,
        apartments,
        townhouses
    FROM {{ ref('stg_building_consents') }}
),

-- join rental + mortgage on year+month
joined AS (
    SELECT
        r.region,
        r.period,
        DATE_TRUNC('year', r.period)            AS consent_year_date,
        YEAR(r.period)                          AS year,
        MONTH(r.period)                         AS month,

        -- rental metrics
        r.rental_price_index,
        r.annual_change_pct                     AS rental_annual_change_pct,

        -- mortgage metrics
        m.one_year_special_rate,

        -- affordability index
        -- higher = less affordable
        -- rental price index divided by mortgage rate
        CASE
            WHEN m.one_year_special_rate IS NOT NULL
            AND  m.one_year_special_rate > 0
            THEN ROUND(r.rental_price_index / m.one_year_special_rate, 2)
            ELSE NULL
        END                                     AS affordability_index

    FROM rental r
    LEFT JOIN mortgage m
        ON DATE_TRUNC('month', r.period) = DATE_TRUNC('month', m.rate_date)
),

-- add supply context from building consents
final AS (
    SELECT
        j.region,
        j.period,
        j.year,
        j.month,
        j.rental_price_index,
        j.rental_annual_change_pct,
        j.one_year_special_rate,
        j.affordability_index,

        -- supply metrics from consents (yearly grain joined to monthly)
        c.all_dwellings          AS annual_consents_all_dwellings,
        c.houses                 AS annual_consents_houses,
        c.apartments             AS annual_consents_apartments,
        c.townhouses             AS annual_consents_townhouses,

        -- supply pressure flag
        CASE
            WHEN j.rental_annual_change_pct > 5
            AND  c.all_dwellings < 40000
            THEN 'High Pressure'
            WHEN j.rental_annual_change_pct > 2
            THEN 'Moderate Pressure'
            ELSE 'Low Pressure'
        END                      AS supply_pressure

    FROM joined j
    LEFT JOIN consents c
        ON j.year = c.consent_year
)

SELECT * FROM final
ORDER BY region, period