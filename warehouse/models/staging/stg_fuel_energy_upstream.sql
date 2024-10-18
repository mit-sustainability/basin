WITH loss AS (
    SELECT
        gl_account_key,
        start_date,
        billing_fy,
        level2_category,
        level3_category,
        number_of_units,
        unit_of_measure,
        ghg * 0.051 AS mtco2e -- MtCO2e
    FROM {{ source("raw", "purchased_energy") }}
    WHERE
        level3_category = 'Electricity'
        AND number_of_units != 0
),

upstream_e AS (
    SELECT
        gl_account_key,
        start_date,
        billing_fy,
        level2_category,
        level3_category,
        number_of_units,
        unit_of_measure,
        number_of_units * 0.0000668 AS mtco2e -- MtCO2e
    FROM {{ source("raw", "purchased_energy") }}
    WHERE
        level3_category = 'Electricity'
        AND number_of_units != 0

),

gas AS (
    SELECT
        gl_account_key,
        start_date,
        billing_fy,
        level2_category,
        level3_category,
        number_of_units,
        unit_of_measure,
        CASE
            WHEN unit_of_measure = 'THM'
                THEN number_of_units * 1.036 * 2.831685
            ELSE number_of_units * 2.831685
        END AS cubic_meters
    FROM {{ source("raw", "purchased_energy") }}
    WHERE
        level3_category = 'Gas'
        AND number_of_units != 0
),

oil AS (
    SELECT
        gl_account_key,
        start_date,
        billing_fy,
        level2_category,
        level3_category,
        number_of_units,
        unit_of_measure,
        number_of_units * 3.785 AS liters
    FROM {{ source("raw", "purchased_energy") }}
    WHERE
        level3_category IN ('Fuel Oil #2', 'Fuel Oil #6')
        AND number_of_units != 0
),

fuel AS (

    SELECT
        gl_account_key,
        start_date,
        billing_fy,
        level2_category,
        level3_category,
        number_of_units,
        unit_of_measure,
        cubic_meters * 0.0003366 AS mtco2e
    FROM gas

    UNION ALL
    SELECT
        gl_account_key,
        start_date,
        billing_fy,
        level2_category,
        level3_category,
        number_of_units,
        unit_of_measure,
        liters * 0.00069539 AS mtco2e
    FROM oil
),

combined AS (
    SELECT * FROM loss
    UNION ALL
    SELECT * FROM upstream_e
    UNION ALL
    SELECT * FROM fuel
),

SELECT
    billing_fy,
    sum(mtco2e) AS mtco2e,
    3 AS "scope",
    'Fuel and energy related' AS category,
    current_timestamp AS last_update
FROM combined
GROUP BY billing_fy
ORDER BY billing_fy
