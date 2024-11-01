WITH latest_cpi AS (
    SELECT value AS latest_cpi
    FROM raw.annual_cpi_index
    ORDER BY year DESC
    LIMIT 1
),

attached AS (
    SELECT
        d.*,
        '230301' AS eeio_code,
        'Maintenance material and Services' AS expense_type,
        COALESCE(aci.value, lc.latest_cpi) AS cpi,
        d."Work Orders Within DOF"
        + d."Sales Work Orders"
        + d."DOF Ops Costs Outside of Wos" AS total

    FROM {{ source('raw', 'dof_maintenance_cost') }} AS d
    LEFT JOIN raw.annual_cpi_index AS aci
        ON d.fiscal_year = aci.year
    CROSS JOIN latest_cpi AS lc
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
        total,
        fiscal_year,
        expense_type,
        total * (SELECT cpi FROM target_cpi) / cpi AS cost_inflation_adjusted_to_2021,
        emission_factor
    FROM factor
)

SELECT
    fiscal_year,
    expense_type,
    cost_inflation_adjusted_to_2021 AS expense_2021_equivalent,
    cost_inflation_adjusted_to_2021 * emission_factor / 1000 AS ghg_emission
FROM adjusted
