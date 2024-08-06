{% set cnd_split = 0.5 %}
{% set residual = 0.22 %}
{% set hard_split = 0.1 %}

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
        END AS emission_factor
    FROM roll AS r
    LEFT JOIN ef AS e ON r.group_id = e.group_id
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

-- scale recycling based on waste audit 22% contamination
recycling AS (
    SELECT
        service_month,
        material,
        tons,
        tons * (1 - {{ residual }}) AS tons_new,
        tons * {{ residual }} AS to_trash,
        emission_factor
    FROM
        ton_month
    WHERE
        material = 'Recycling'


),

-- 90/10 split hard_recycling (Trash/Recycling)
hard_recycle AS (
    SELECT
        service_month,
        material,
        tons * {{ hard_split }} AS tons_new,
        tons * (1 - {{ hard_split }}) AS to_recycling,
        emission_factor
    FROM
        ton_month
    WHERE
        material = 'Hard-to-Recycle Materials'
),

-- 50/50 split C & D Trash/Recycling
cnd AS (
    SELECT
        service_month,
        material,
        tons * {{ cnd_split }} AS tons_new,
        tons * (1 - {{ cnd_split }}) AS to_recycling,
        emission_factor
    FROM
        ton_month
    WHERE
        material = 'C & D'
),


-- add partial hard-to-recycle and C&D to recycling
recycling_adjusted AS (
    SELECT
        r.service_month,
        r.material,
        r.tons_new + coalesce(h.to_recycling, 0) + coalesce(c.to_recycling, 0) AS tons_new,
        r.emission_factor
    FROM recycling AS r
    LEFT JOIN hard_recycle AS h
        ON r.service_month = h.service_month
    LEFT JOIN cnd AS c
        ON r.service_month = c.service_month
),

-- trash and organics go back to the trash stream
trash_adjusted AS (
    SELECT
        t.service_month,
        t.material,
        t.tons + r.to_trash AS tons_new,
        t.emission_factor
    FROM
        ton_month AS t
    LEFT JOIN
        recycling AS r
        ON
            t.service_month = r.service_month
    WHERE
        t.material = 'Trash'

),


-- combine all streams
adjusted AS (
    SELECT
        service_month,
        material,
        tons_new AS tons,
        emission_factor
    FROM recycling_adjusted

    UNION ALL

    SELECT
        service_month,
        material,
        tons_new AS tons,
        emission_factor
    FROM trash_adjusted

    UNION ALL

    SELECT
        service_month,
        material,
        tons_new AS tons,
        emission_factor
    FROM hard_recycle

    UNION ALL

    SELECT
        service_month,
        material,
        tons_new AS tons,
        emission_factor
    FROM cnd

    UNION ALL

    SELECT
        service_month,
        material,
        tons,
        emission_factor
    FROM ton_month
    WHERE material NOT IN ('Recycling', 'Trash', 'Hard-to-Recycle Materials', 'C & D')
),

-- calculate GHG emission
ghg AS (
    SELECT
        ae.service_month,
        ae.material,
        ae.tons AS tons_adjusted,
        tm.tons AS tons_original,
        ae.emission_factor,
        ae.tons * ae.emission_factor AS co2eq,
        CASE
            WHEN extract(MONTH FROM ae."service_month") < 7
                THEN extract(YEAR FROM ae."service_month")
            ELSE extract(YEAR FROM ae."service_month") + 1
        END AS fiscal_year,
        extract(YEAR FROM ae."service_month") AS "year"
    FROM adjusted AS ae
    LEFT JOIN ton_month AS tm
        ON
            ae.service_month = tm.service_month
            AND ae.material = tm.material
)

SELECT
    service_month,
    material,
    tons_original AS tons,
    tons_adjusted AS tons_adjusted,
    co2eq,
    "year"
FROM ghg
ORDER BY service_month
