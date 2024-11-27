WITH entries AS (
    -- Process manual entries for scope 1 and 2 categories
    SELECT
        CASE
            WHEN category LIKE '1.1%' THEN '1. Direct emissions'
            WHEN category LIKE '2.1%' THEN '2. Indirect electricity'
            ELSE category
        END AS category,
        emission,
        fiscal_year,
        "scope",
        last_update::timestamp AS last_update
    FROM {{ source("raw", "ghg_manual_entries") }}
),

business AS (
    -- Aggregate business travel emissions by fiscal year
    SELECT
        '3.6 Business Travel' AS category,
        SUM(total_mtco2) AS emission,
        fiscal_year,
        3 AS "scope",
        MAX(last_update)::timestamp AS last_update
    FROM {{ ref("travel_FY_exp_group_summary") }}
    GROUP BY fiscal_year
),


construction AS (
    -- Aggregate construction-related emissions by fiscal year
    SELECT
        '3.2 Construction' AS category,
        SUM(ghg_emission) AS emission,
        fiscal_year,
        3 AS "scope",
        MAX(last_update)::timestamp AS last_update
    FROM {{ ref("construction_expense_emission") }}
    GROUP BY fiscal_year
),

waste AS (
    -- Aggregate waste-related emissions by fiscal year
    SELECT
        '3.5 Waste' AS category,
        SUM(co2eq) AS emission,
        fiscal_year,
        3 AS "scope",
        MAX(last_update)::timestamp AS last_update
    FROM {{ ref("final_waste_emission") }}
    GROUP BY fiscal_year
),

pgs AS (
    -- Aggregate purchased goods and services emissions by fiscal year
    SELECT
        '3.1 Purchased Goods and Services' AS category,
        SUM(mtco2) AS emission,
        fiscal_year,
        3 AS "scope",
        CURRENT_TIMESTAMP AS last_update
    FROM {{ ref("stg_purchased_goods_invoice") }}
    GROUP BY fiscal_year
),

cdr AS (
    -- Extract purchased energy emissions with scope differentiation
    SELECT
        ghg,
        billing_fy AS fiscal_year,
        last_update::timestamp AS last_update,
        CASE
            WHEN level3_category IN ('Gas', 'Fuel Oil #2', 'Fuel Oil #6') THEN 1
            WHEN level3_category = 'Electricity' THEN 2
        END AS scope
    FROM {{ source("raw", "purchased_energy") }}
),

energy AS (
    -- Aggregate purchased energy emissions by fiscal year and scope
    SELECT
        CASE
            WHEN scope = 1 THEN '1. Direct emissions'
            WHEN scope = 2 THEN '2. Indirect electricity'
        END AS category,
        SUM(ghg) AS emission,
        fiscal_year,
        scope,
        MAX(last_update)::timestamp AS last_update
    FROM cdr
    GROUP BY fiscal_year, scope
),

fera AS (
    -- Extract emissions from upstream fuel and energy activities
    SELECT
        '3.3 Fuel and energy-related activities' AS category,
        mtco2e AS emission,
        billing_fy AS fiscal_year,
        scope,
        last_update::timestamp AS last_update
    FROM {{ ref("stg_fuel_energy_upstream") }}
),

combined AS (
    -- Combine all data sources into a single dataset with source tracking
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update,
        'manual' AS source
    FROM entries
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update,
        'business' AS source
    FROM business
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update,
        'construction' AS source
    FROM construction
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update,
        'waste' AS source
    FROM waste
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update,
        'pgs' AS source
    FROM pgs
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update,
        'energy' AS source
    FROM energy
    UNION ALL
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update,
        'fera' AS source
    FROM fera
),

ranked AS (
    -- Assign rank to sources for prioritization
    SELECT
        category,
        fiscal_year,
        scope,
        emission,
        last_update,
        source,
        ROW_NUMBER() OVER (
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

-- Final aggregated results with prioritized sources
SELECT
    category,
    fiscal_year,
    MAX(scope) AS scope,
    MAX(emission) AS emission,
    MAX(last_update) AS last_update
FROM ranked
WHERE row_num = 1 -- Ensure highest-priority source is used
GROUP BY category, fiscal_year
ORDER BY fiscal_year
