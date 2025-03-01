WITH capita AS (
    SELECT
        fiscal_year,
        CASE
            WHEN category = '3.2 Construction' THEN '3.2 Capital Goods'
            ELSE category
        END AS category,
        emission,
        dlc_name,
        school_area
    FROM {{ ref('stg_dlc_footprint_capita') }}
),

energy AS (
    SELECT
        fiscal_year,
        '1 & 2 Energy' AS category,
        emission,
        dlc_name,
        school_area
    FROM {{ ref('stg_dlc_energy_summary') }}
),

travel AS (
    SELECT
        fiscal_year,
        '3.6 Business Travel' AS category,
        sum(mtco2) AS emission,
        dlc_name,
        max(school_area) AS school_area
    FROM {{ ref('stg_travel_spending') }}
    WHERE dlc_name IS NOT NULL
    GROUP BY fiscal_year, dlc_name
),

pgs AS (
    SELECT
        fiscal_year,
        '3.1 Purchased Goods and Services' AS category,
        sum(mtco2) AS emission,
        dlc_name
    FROM {{ ref('stg_purchased_goods_invoice') }}
    WHERE dlc_name IS NOT NULL
    GROUP BY fiscal_year, dlc_name
    ORDER BY fiscal_year
),

pgs_dlc AS (
    SELECT
        pgs.fiscal_year,
        pgs.category,
        pgs.emission,
        pgs.dlc_name,
        c.school_area
    FROM pgs
    LEFT JOIN {{ ref('stg_cost_object_rollup') }} AS c
        ON pgs.dlc_name = c.dlc_name
),

recording AS (
    SELECT e.* FROM energy AS e
    UNION ALL
    SELECT c.* FROM capita AS c
    UNION ALL
    SELECT p.* FROM pgs_dlc AS p
    UNION ALL
    SELECT t.* FROM travel AS t
)

SELECT
    row_number() OVER (ORDER BY fiscal_year, category) AS id,
    *
FROM recording
