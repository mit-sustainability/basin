WITH original AS (

    SELECT
        "year",
        material,
        sum(tons) AS tons,
        sum(co2eq) AS co2eq,
        row_number() OVER (ORDER BY year, material) AS num_id
    FROM {{ ref('stg_waste_emission') }}
    GROUP BY "year", material
    ORDER BY "year"
),

adjusted AS (
    SELECT
        "year",
        material,
        sum(tons) AS tons,
        sum(co2eq) AS co2eq,
        row_number() OVER (ORDER BY year, material) AS num_id
    FROM {{ ref('stg_waste_emission_audit') }}
    GROUP BY "year", material
    ORDER BY "year"
)

SELECT
    o.year,
    o.material,
    o.tons,
    o.co2eq,
    a.tons AS tons_adjusted,
    a.co2eq AS co2eq_adjusted
FROM original AS o
INNER JOIN adjusted AS a ON o.num_id = a.num_id
