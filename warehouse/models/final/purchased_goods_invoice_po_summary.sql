SELECT
    po_number,
    fiscal_year,
    string_agg(DISTINCT "level_3", ', ') AS level_3,
    sum(mtco2) AS mtco2,
    sum(inflated_spend) AS spend_usd,
    max(level_2) AS level_2,
    max(level_1) AS level_1,
    string_agg(DISTINCT "description", ', ') AS "description",
    current_timestamp AS last_update
FROM {{ref('stg_purchased_goods_invoice')}}
GROUP BY po_number, fiscal_year
ORDER BY fiscal_year
