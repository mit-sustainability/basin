WITH unique_dlc AS (
    SELECT DISTINCT
        dlc_name,
        school_area
    FROM {{ref('stg_cost_object_rollup')}}
),

pns AS (
    SELECT
        fiscal_year,
        dlc_name,
        level_3,
        MAX(level_2) AS level_2,
        SUM(mtco2) AS emission,
        SUM(inflated_spend) AS inflated_spend
    FROM {{ref('stg_purchased_goods_invoice')}}
    GROUP BY fiscal_year, dlc_name, level_3
)

SELECT
    p.*,
    c.school_area
FROM pns AS p
LEFT JOIN unique_dlc AS c
    ON p.dlc_name = c.dlc_name
ORDER BY p.fiscal_year, p.dlc_name
