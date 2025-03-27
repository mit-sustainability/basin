WITH cdr AS (
    -- Extract Energy related emissions by buildings
    SELECT
        ghg,
        billing_fy AS fiscal_year,
        building_number,
        last_update::timestamp AS last_update
    FROM {{ source("raw", "energy_distribution") }}
)

SELECT
    building_number,
    fiscal_year,
    sum(ghg) AS emission,
    max(last_update)::timestamp AS last_update
FROM cdr
GROUP BY fiscal_year, building_number
