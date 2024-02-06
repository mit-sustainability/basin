{{ config(materialized='table', sort='trip_end_date') }}

WITH lab AS (
    SELECT
        expense_type,
        expense_amount,
        cost_object,
        trip_end_date,
        CASE
            WHEN EXTRACT(MONTH FROM trip_end_date) < 7
                THEN EXTRACT(YEAR FROM trip_end_date)
            ELSE EXTRACT(YEAR FROM trip_end_date) + 1
        END AS fiscal_year,
        ROW_NUMBER() OVER (ORDER BY trip_end_date) AS transaction_id,
        EXTRACT(YEAR FROM trip_end_date) AS cal_year
    FROM {{ source('raw', 'travel_spending') }}
),

target_cpi AS (
    SELECT
        year,
        value
    FROM raw.annual_cpi_index ORDER BY year DESC LIMIT 1
), --checked

attached AS (
    SELECT
        l.*,
        COALESCE(cpi.value, (SELECT value FROM target_cpi)) AS src
    FROM lab AS l
    LEFT JOIN {{ source('raw', 'annual_cpi_index') }} AS cpi
        ON l.cal_year = cpi.year
),

adjusted AS (
    SELECT
        transaction_id,
        expense_amount * (SELECT value FROM target_cpi) / src AS inflated_exp_amount,
        cal_year,
        fiscal_year,
        expense_amount,
        trip_end_date,
        expense_type,
        CAST(cost_object AS INTEGER) AS cost_object
    FROM attached
),

dlc AS (
    SELECT
        cost_object,
        dlc_name,
        school_area
    FROM {{ref('stg_cost_object_rollup')}}
),

tagged AS (
    SELECT
        adj.*,
        dlc.dlc_name,
        dlc.school_area
    FROM adjusted AS adj
    LEFT JOIN dlc
        ON adj.cost_object = dlc.cost_object
),

cat AS (
    SELECT
        t.*,
        COALESCE(c.category, 'Other') AS expense_group,
        ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY LENGTH(c.type) DESC) AS rn1
    FROM tagged AS t
    LEFT JOIN {{ source('raw', 'expense_category_mapper') }} AS c
        ON LOWER(t.expense_type) LIKE '%' || c.type || '%'
),

em AS (
    SELECT
        c.*,
        COALESCE(e.emission_category, 'misc.') AS transport_mode,
        ROW_NUMBER()
            OVER (PARTITION BY c.transaction_id ORDER BY LENGTH(e.expense_type) DESC)
        AS rn2
    FROM (SELECT * FROM cat WHERE rn1 = 1) AS c
    LEFT JOIN {{ source('raw', 'expense_emission_mapper') }} AS e
        ON LOWER(c.expense_type) LIKE '%' || e.expense_type || '%'
),

factor AS (
    SELECT
        e.*,
        m."CO2_factor" AS co2_factor
    FROM (SELECT * FROM em WHERE rn2 = 1) AS e
    INNER JOIN {{ source('raw', 'mode_co2_mapper') }} AS m
        ON e.transport_mode = m.transport_mode
)

SELECT
    f.expense_amount,
    f.expense_type,
    f.trip_end_date,
    f.cost_object,
    f.fiscal_year,
    f.cal_year,
    f.inflated_exp_amount,
    f.dlc_name,
    f.school_area,
    f.expense_group,
    f.transport_mode,
    f.co2_factor,
    f.co2_factor * f.inflated_exp_amount AS mtco2
FROM factor AS f
