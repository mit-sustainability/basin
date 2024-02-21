SELECT
    fiscal_year,
    expense_type,
    expense_2021_equivalent / 1e6 AS expense_2021,
    ghg_emission
FROM {{ref('stg_dof_maintenance_cost')}}

UNION

SELECT
    fiscal_year,
    expense_type,
    expense_in_million_2021_equivalent,
    ghg_emission
FROM {{ref('stg_construction_expense')}}

ORDER BY fiscal_year
