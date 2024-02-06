SELECT
    dlc_name,
    fiscal_year,
    expense_group,
    SUM(mtco2) AS group_mtco2,
    SUM(expense_amount) AS group_expense_amount,
    CASE
        WHEN SUM(SUM(mtco2)) OVER (PARTITION BY dlc_name, fiscal_year) = 0 THEN 0
        ELSE SUM(mtco2) / SUM(SUM(mtco2)) OVER (PARTITION BY dlc_name, fiscal_year)
    END AS share_of_total_mtco2,
    CASE
        WHEN SUM(SUM(expense_amount)) OVER (PARTITION BY dlc_name, fiscal_year) = 0 THEN 0
        ELSE
            SUM(expense_amount) / SUM(SUM(expense_amount)) OVER (PARTITION BY dlc_name, fiscal_year)
    END AS share_of_total_expense_amount
FROM {{ref('stg_travel_spending')}}
GROUP BY
    dlc_name,
    fiscal_year,
    expense_group
ORDER BY
    dlc_name,
    fiscal_year,
    expense_group
