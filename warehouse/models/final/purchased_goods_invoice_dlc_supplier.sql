WITH unique_dlc AS (
    SELECT DISTINCT
        dlc_name,
        school_area
    FROM {{ ref('stg_cost_object_rollup')}}
),

pns AS (
    SELECT
        fiscal_year,
        dlc_name,
        supplier,
        SUM(mtco2) AS emission,
        SUM(inflated_spend) AS inflated_spend
    FROM {{ ref('stg_purchased_goods_invoice')}}
    GROUP BY fiscal_year, dlc_name, supplier, supplier_number
)

SELECT
    p.*,
    c.school_area
FROM pns AS p
LEFT JOIN unique_dlc AS c
    ON p.dlc_name = c.dlc_name
ORDER BY p.fiscal_year, p.dlc_name, p.supplier
