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

current_cpi AS (
    SELECT
        year,
        value
    FROM {{ source('raw', 'annual_cpi_index') }} ORDER BY year DESC LIMIT 1
),

target_cpi AS (
    SELECT
        year,
        value
    FROM {{ source('raw', 'annual_cpi_index') }}
    WHERE "year" = 2012
),

attached AS (
    SELECT
        l.*,
        COALESCE(cpi.value, (SELECT value FROM current_cpi)) AS src
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


em AS (
    SELECT
        t.*,
        COALESCE(e.emission_category, 'misc.') AS transport_mode,
        ROW_NUMBER()
            OVER (PARTITION BY t.transaction_id ORDER BY LENGTH(e.expense_type) DESC)
        AS rn
    FROM tagged AS t
    LEFT JOIN {{ source('raw', 'expense_emission_mapper') }} AS e
        ON LOWER(t.expense_type) LIKE '%' || e.expense_type || '%'
),

ef AS (
    SELECT
        CASE
            WHEN "Code" = '721000' THEN 'housing'
            WHEN "Code" = '311990' THEN 'meals'
            WHEN "Code" = '481000' THEN 'air'
            WHEN "Code" = '482000' THEN 'train'
            WHEN "Code" = '483000' THEN 'ferry'
            WHEN "Code" = '485000' THEN 'car'
            ELSE 'misc.'
        END AS transport_mode,
        emission_factor
    FROM {{ source('raw', 'emission_factor_useeio_v2') }}
    WHERE "Code" IN ('721000', '311990', '481000', '482000', '483000', '485000')
),

factor AS (
    SELECT
        e.*,
        COALESCE(ef."emission_factor", 0) AS co2_factor
    FROM (SELECT * FROM em WHERE rn = 1) AS e
    LEFT JOIN ef
        ON e.transport_mode = ef.transport_mode
),

-- Add expense group for visualization from transport mode
cat AS (
    SELECT
        *,
        CASE
            WHEN transport_mode = 'air' THEN 'Air Travel'
            WHEN transport_mode = 'housing' THEN 'Accommodations'
            WHEN transport_mode = 'meals' THEN 'Food'
            WHEN transport_mode IN ('car', 'train', 'ferry') THEN 'Ground Travel'
            ELSE 'Other'
        END AS expense_group
    FROM factor
)

SELECT
    c.expense_amount,
    c.expense_type,
    c.trip_end_date,
    c.cost_object,
    c.fiscal_year,
    c.cal_year,
    c.inflated_exp_amount,
    c.dlc_name,
    c.school_area,
    c.expense_group,
    c.transport_mode,
    c.co2_factor,
    c.co2_factor * c.inflated_exp_amount / 1000 AS mtco2
FROM cat AS c
