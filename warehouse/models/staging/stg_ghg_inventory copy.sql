WITH manual AS (
    SELECT
        category,
        emission,
        fiscal_year,
        "scope",
        last_update
    FROM {{ source("raw", "ghg_manual_entries") }}
),

business AS (
    SELECT
        '3.6 Business Travel' AS category,
        sum(total_mtco2) AS emission,
        fiscal_year,
        3 AS "scope",
        max(last_update) AS last_update
    FROM {{ ref("travel_FY_exp_group_summary") }}
    GROUP BY fiscal_year
),

construction AS (
    SELECT
        '3.2 Construction' AS category,
        sum(ghg_emission) AS emission,
        fiscal_year,
        3 AS "scope",
        max(last_update) AS last_update
    FROM {{ ref("construction_expense_emission") }}
    GROUP BY fiscal_year
),

waste AS (
    SELECT
        '3.5 Waste' AS category,
        sum(co2eq) AS emission,
        "year" AS fiscal_year,
        3 AS "scope",
        max(last_update) AS last_update
    FROM {{ ref("final_waste_emission") }}
    GROUP BY fiscal_year
),

pgs AS (
    SELECT
        '3.1 Purchased Goods and Services' AS category,
        sum(ghg) / 1000 AS emission, -- mtco2
        fiscal_year,
        3 AS "scope",
        max(last_update) AS last_update
    FROM {{ ref("stg_purchased_goods_invoice") }}
    GROUP BY fiscal_year
),

-- purchased energy from energize-mit
cdr AS (
    SELECT
        ghg,
        billing_fy,
        last_update,
        CASE
            WHEN level3_category IN ('Gas', 'Fuel Oil #2', 'Fuel Oil #6') THEN 1
            WHEN level3_category = 'Electricity' THEN 2
        END AS scope
    FROM {{ source("raw", "purchased_energy") }}
),

energy AS (
    SELECT
        CASE
            WHEN scope = 1 THEN '1. Direct emissions'
            WHEN scope = 2 THEN '2. Indirect electricity'
        END AS category,
        sum(ghg) AS emission,
        billing_fy AS fiscal_year,
        scope,
        max(last_update) AS last_update
    FROM cdr
    GROUP BY billing_fy, scope
),

combined AS (
    SELECT * FROM manual
    UNION ALL
    SELECT * FROM business
    UNION ALL
    SELECT * FROM construction
    UNION ALL
    SELECT * FROM waste
    UNION ALL
    SELECT * FROM pgs
    UNION ALL
    SELECT * FROM energy
)

SELECT
    category,
    fiscal_year,
    scope,
    avg(emission) AS emission,
    max(last_update) AS last_update
FROM combined
GROUP BY category, fiscal_year, scope
ORDER BY fiscal_year
