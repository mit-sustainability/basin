WITH fy_campus_utility AS (
    SELECT
        fiscal_year,
        utility_type,
        sum(total_cost) AS total_cost,
        sum(total_usage) AS total_usage,
        sum(total_mmbtu) AS total_mmbtu,
        sum(total_gross_area) AS total_gross_area
    FROM {{ ref('fy_summary_utility') }}
    WHERE utility_type IN ('Electricity', 'Gas')
    GROUP BY fiscal_year, utility_type
)


SELECT
    fiscal_year,
    utility_type,
    total_cost,
    total_usage,
    total_mmbtu,
    total_mmbtu / total_gross_area AS energy_intensity
FROM fy_campus_utility
