-- Adapt manual entry scope 1 and 2 categories
WITH entries AS (
    SELECT
        CASE
            WHEN category LIKE '1.%' THEN '1. Direct emissions'
            WHEN category LIKE '2.%' THEN '2. Indirect electricity'
            ELSE category
        END AS category,
        emission,
        fiscal_year,
        "scope",
        last_update
    FROM {{ source("raw", "ghg_manual_entries") }}
),

-- Aggregate manually entried scope 1 and 2
rolled AS (
    SELECT
        category,
        sum(emission) AS emission,
        fiscal_year,
        max("scope") AS "scope",
        max(last_update) AS last_update
    FROM entries
    GROUP BY category, fiscal_year
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
        fiscal_year,
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
        current_timestamp AS "last_update"
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

fera AS (
    SELECT
        '3.3 Fuel and energy-related activities' AS category,
        mtco2e AS emission,
        billing_fy AS fiscal_year,
        scope,
        last_update
    FROM {{ ref("stg_fuel_energy_upstream") }}
),

combined AS (
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update::timestamp AS last_timestamp,
        'manual' AS source
    FROM rolled
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update::timestamp AS last_timestamp,
        'business' AS source
    FROM business
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update::timestamp AS last_timestamp,
        'construction' AS source
    FROM construction
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update::timestamp AS last_timestamp,
        'waste' AS source
    FROM waste
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update::timestamp AS last_timestamp,
        'pgs' AS source
    FROM pgs
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update::timestamp AS last_timestamp,
        'energy' AS source
    FROM energy
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update::timestamp AS last_timestamp,
        'fera' AS source
    FROM fera
),

ranked AS (
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_timestamp AS last_update,
        source,
        row_number() OVER (
            PARTITION BY category, fiscal_year
            ORDER BY
                CASE
                    WHEN source = 'manual' THEN 1
                    WHEN source = 'business' THEN 2
                    WHEN source = 'construction' THEN 3
                    WHEN source = 'waste' THEN 4
                    WHEN source = 'pgs' THEN 5
                    WHEN source = 'energy' THEN 6
                    WHEN source = 'fera' THEN 7
                    ELSE 8
                END
        ) AS row_num
    FROM combined
)

SELECT
    category,
    fiscal_year,
    max(scope) AS scope,
    max(emission) AS emission,
    max(last_update) AS last_update
FROM ranked
WHERE row_num = 1 -- Ensure manual overwrites other sources
GROUP BY category, fiscal_year
ORDER BY fiscal_year
