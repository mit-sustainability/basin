SELECT
    fiscal_year,
    level_3,
    sum(ghg) / 1000 AS mtco2,
    sum(inflated_spend) AS spend_usd,
    max(level_2) AS level_2,
    max(level_1) AS level_1,
    (sum(ghg) / sum(sum(ghg)) OVER (PARTITION BY fiscal_year)) * 100 AS share_mtco2,
    (sum(inflated_spend) / sum(sum(inflated_spend)) OVER (PARTITION BY fiscal_year))
    * 100 AS share_spend
FROM {{ref('stg_purchased_goods_invoice')}}
GROUP BY fiscal_year, level_3
ORDER BY fiscal_year, level_3
