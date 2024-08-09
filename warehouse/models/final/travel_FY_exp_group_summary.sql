SELECT
    fiscal_year,
    expense_group,
    SUM(inflated_exp_amount) AS total_expense_amount,
    SUM(inflated_exp_amount)
    / SUM(SUM(inflated_exp_amount)) OVER (PARTITION BY fiscal_year) AS expense_amount_share,
    SUM(mtco2) AS total_mtco2,
    SUM(mtco2) / SUM(SUM(mtco2)) OVER (PARTITION BY fiscal_year) AS mtco2_share,
    CURRENT_TIMESTAMP AS "last_update"
FROM {{ref('stg_travel_spending')}}
GROUP BY fiscal_year, expense_group
