SELECT
    "year",
    material,
    sum(tons) AS tons,
    sum(co2eq) AS co2eq
FROM {{ ref('stg_waste_emission') }}
GROUP BY "year", material
ORDER BY "year"
