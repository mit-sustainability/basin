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
            ELSE 1
        END AS group_id
    FROM {{ source("raw", "waste_emission_factors_EPA") }}
    WHERE material IN ('Mixed MSW', 'Yard Trimmings', 'Food Waste', 'Mixed Organics')
),

-- Integrate waste audit material share
ef_recycle AS (
    SELECT
        material,
        recycled,
        CASE
            WHEN material = 'Mixed Paper (primarily from offices)' THEN 0.39
            WHEN material = 'Mixed Metals' THEN 0.07
            WHEN material = 'Mixed Plastics' THEN 0.27
            ELSE 0.27 -- Glass
        END AS weight
    FROM {{ source("raw", "waste_emission_factors_EPA") }}
    WHERE
        material IN (
            'Mixed Paper (primarily from offices)', 'Glass', 'Mixed Metals', 'Mixed Plastics'
        )
),

ef_ext AS (
    SELECT * FROM ef
    UNION ALL
    SELECT
        'Weighted Recycle' AS material,
        (SELECT sum(recycled * weight) AS mixed_recycle FROM ef_recycle) AS recycled,
        0 AS landfilled,
        0 AS combusted,
        0 AS composted,
        2 AS group_id
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
        END AS emission_factor
    FROM roll AS r
    LEFT JOIN ef_ext AS e ON r.group_id = e.group_id
),


-- bin to calendar month
ton_month AS (
    SELECT
        date_trunc('month', service_date)::date AS service_month,
        material,
        sum(tons) AS tons,
        max(emission_factor) AS emission_factor
    FROM attached_ef
    GROUP BY date_trunc('month', service_date)::date, material
    ORDER BY service_month
),

-- scale recycling based on waste audit
recycling_scaled AS (
    SELECT
        service_month,
        material,
        tons,
        tons * 0.68 AS tons_scaled,
        tons * 0.32 AS sorted_trash,
        emission_factor
    FROM
        ton_month
    WHERE
        material = 'Recycling'


),

-- trash and organics go back to the trash stream
trash_updated AS (
    SELECT
        t.service_month,
        t.material,
        t.tons + r.sorted_trash AS updated_tons,
        t.emission_factor
    FROM
        ton_month AS t
    LEFT JOIN
        recycling_scaled AS r
        ON
            t.service_month = r.service_month
    WHERE
        t.material = 'Trash'
),

adjusted AS (
    SELECT
        service_month,
        material,
        tons_scaled AS tons,
        emission_factor
    FROM recycling_scaled

    UNION ALL

    SELECT
        service_month,
        material,
        updated_tons AS tons,
        emission_factor
    FROM trash_updated

    UNION ALL

    SELECT
        service_month,
        material,
        tons,
        emission_factor
    FROM ton_month
    WHERE material NOT IN ('Recycling', 'Trash')
),

-- calculate GHG emission
ghg AS (
    SELECT
        ae.service_month,
        ae.material,
        ae.tons,
        ae.emission_factor,
        ae.tons * ae.emission_factor AS co2eq,
        CASE
            WHEN extract(MONTH FROM ae."service_month") < 7
                THEN extract(YEAR FROM ae."service_month")
            ELSE extract(YEAR FROM ae."service_month") + 1
        END AS fiscal_year,
        extract(YEAR FROM ae."service_month") AS "year"
    FROM adjusted AS ae
)

SELECT
    service_month,
    material,
    tons,
    emission_factor,
    co2eq,
    "year"
FROM ghg
ORDER BY service_month
