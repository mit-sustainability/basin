SELECT
    fiscal_year,
    material,
    sum(tons) AS tons,
    sum(co2eq) AS co2eq,
    current_timestamp AS "last_update"
FROM {{ ref('stg_waste_emission') }}
GROUP BY fiscal_year, material
ORDER BY fiscal_year
