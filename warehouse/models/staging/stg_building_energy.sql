WITH cdr AS (
    -- Extract purchased energy emissions by buildings
    SELECT
        ghg,
        billing_fy AS fiscal_year,
        building_number,
        last_update::timestamp AS last_update
    FROM {{ source("raw", "purchased_energy") }}
)

SELECT
    building_number,
    fiscal_year,
    sum(ghg) AS emission,
    max(last_update)::timestamp AS last_update
FROM cdr
GROUP BY fiscal_year, building_number
