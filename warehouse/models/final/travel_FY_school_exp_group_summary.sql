SELECT
    fiscal_year,
    school_area,
    expense_group,
    SUM(inflated_exp_amount) AS school_expense_total,
    SUM(inflated_exp_amount)
    / SUM(SUM(inflated_exp_amount)) OVER (PARTITION BY fiscal_year) AS school_expense_amount_share,
    SUM(mtco2) AS school_mtco2_total,
    SUM(mtco2) / SUM(SUM(mtco2)) OVER (PARTITION BY fiscal_year) AS school_mtco2_share
FROM {{ref('stg_travel_spending')}}
GROUP BY fiscal_year, school_area, expense_group
