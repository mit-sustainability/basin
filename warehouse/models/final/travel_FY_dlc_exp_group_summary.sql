SELECT
    fiscal_year,
    dlc_name,
    expense_group,
    SUM(inflated_exp_amount) AS dlc_expense_total,
    SUM(inflated_exp_amount)
    / SUM(SUM(inflated_exp_amount)) OVER (PARTITION BY fiscal_year) AS dlc_expense_amount_share,
    SUM(mtco2) AS dlc_mtco2_total,
    SUM(mtco2) / SUM(SUM(mtco2)) OVER (PARTITION BY fiscal_year) AS dlc_mtco2_share
FROM {{ref('stg_travel_spending')}}
GROUP BY fiscal_year, dlc_name, expense_group
