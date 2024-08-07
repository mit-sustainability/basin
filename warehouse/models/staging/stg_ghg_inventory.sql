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
        current_timestamp AS "last_update"
    FROM {{ ref("travel_FY_exp_group_summary") }}
    GROUP BY fiscal_year
),

construction AS (
    SELECT
        '3.2 Construction' AS category,
        sum(ghg_emission) AS emission,
        fiscal_year,
        3 AS "scope",
        current_timestamp AS "last_update"
    FROM {{ ref("construction_expense_emission") }}
    GROUP BY fiscal_year
),

waste AS (
    SELECT
        '3.5 Waste' AS category,
        sum(co2eq) AS emission,
        "year" AS fiscal_year,
        3 AS "scope",
        current_timestamp AS "last_update"
    FROM {{ ref("final_waste_emission") }}
    GROUP BY fiscal_year
),


combined AS (
    SELECT * FROM manual
    UNION ALL
    SELECT * FROM business
    UNION ALL
    SELECT * FROM construction
    UNION ALL
    SELECT * FROM waste
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
