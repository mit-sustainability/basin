WITH ef AS (
    SELECT
        material,
        recycled,
        landfilled,
        combusted,
        composted,
        CASE
            WHEN material = 'Yard Trimmings' THEN 5
            WHEN material = 'Mixed Organics' THEN 4
            WHEN material = 'Food Waste' THEN 3
            WHEN material = 'Mixed Recyclables' THEN 2
            ELSE 1
        END AS group_id
    FROM {{ source("raw", "waste_emission_factors_EPA") }}
    WHERE
        material IN (
            'Mixed MSW', 'Yard Trimmings', 'Food Waste', 'Mixed Recyclables', 'Mixed Organics'
        )
),

roll AS (
    SELECT
        *,
        CASE
            WHEN material IN ('Hard-to-Recycle Materials', 'C & D', 'Trash') THEN 1
            WHEN material = 'Recycling' THEN 2
            WHEN material = 'Compost' THEN 3
            WHEN material = 'Other' THEN 4
            ELSE 5
        END AS group_id
    FROM {{ ref("final_waste_recycle") }}
),


-- different category has different combination of landfilled/combusted/composted
attached_ef AS (
    SELECT
        r.service_date,
        r.customer_name,
        r.material,
        r.tons,
        CASE
            WHEN
                r.material IN ('Hard-to-Recycle Materials', 'Other', 'Trash')
                THEN (e.landfilled + e.combusted) / 2
            WHEN r.material = 'C & D' THEN e.landfilled
            WHEN r.material IN ('Compost', 'Yard Waste') THEN e.composted
            ELSE e.recycled
        END AS emission_factors
    FROM roll AS r
    LEFT JOIN ef AS e ON r.group_id = e.group_id
),

ghg AS (
    SELECT
        ae.service_date,
        ae.customer_name,
        ae.material,
        ae.tons,
        ae.emission_factors,
        ae.tons * ae.emission_factors AS co2eq,
        CASE
            WHEN EXTRACT(MONTH FROM ae."service_date") < 7
                THEN EXTRACT(YEAR FROM ae."service_date")
            ELSE EXTRACT(YEAR FROM ae."service_date") + 1
        END AS fiscal_year,
        EXTRACT(YEAR FROM ae."service_date") AS "year"
    FROM attached_ef AS ae
)


SELECT
    service_date,
    material,
    tons,
    emission_factors AS emission_factor,
    co2eq,
    "year"
FROM ghg
WHERE service_date IS NOT NULL
ORDER BY service_date
