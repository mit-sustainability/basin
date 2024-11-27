SELECT
    fiscal_year,
    supplier_number,
    sum(ghg) / 1000 AS mtco2,
    sum(inflated_spend) AS spend_usd,
    string_agg(DISTINCT "level_3", ', ') AS level_3,
    string_agg(DISTINCT "supplier", ', ') AS supplier,
    string_agg(DISTINCT "description", ', ') AS "description",
    current_timestamp AS last_update
FROM {{ref('stg_purchased_goods_invoice')}}
GROUP BY fiscal_year, supplier_number
ORDER BY fiscal_year, supplier_number
