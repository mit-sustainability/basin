WITH expense AS (

    SELECT
        "New Construction" AS expense,
        'New Construction' AS expense_type,
        '233262' AS eeio_code,
        fiscal_year
    FROM {{ source('raw', 'construction_expense') }}

    UNION ALL

    SELECT
        "Renovations and Renewal*" AS expense,
        'Renovation and Renewal' AS expense_type,
        '230301' AS eeio_code,
        fiscal_year
    FROM {{ source('raw', 'construction_expense') }}
    ORDER BY fiscal_year, expense_type
),

attached AS (
    SELECT
        e.*,
        aci.value AS cpi
    FROM expense AS e
    LEFT JOIN {{ source('raw', 'annual_cpi_index') }} AS aci
        ON e.fiscal_year = aci.year
),

factor AS (
    SELECT
        a.*,
        ef."Supply Chain Emission Factors with Margins" AS emission_factor
    FROM attached AS a
    LEFT JOIN {{ source('raw', 'emission_factor_naics') }} AS ef
        ON a.eeio_code = ef."Code"
),

target_cpi AS (
    SELECT cpi
    FROM attached
    WHERE fiscal_year = 2021
    LIMIT 1
),

adjusted AS (
    SELECT
        expense,
        expense_type,
        fiscal_year,
        expense * (SELECT cpi FROM target_cpi) / cpi AS expense_inflation_adjusted,
        emission_factor
    FROM factor
)

SELECT
    a.fiscal_year,
    a.expense_type,
    a.expense_inflation_adjusted AS expense_in_million_2021_equivalent,
    a.emission_factor * a.expense_inflation_adjusted * 1000 AS ghg_emission
FROM adjusted AS a
