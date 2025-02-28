WITH FilteredEmissions AS (
    SELECT
        category,
        fiscal_year,
        SUM(emission) AS total_emission
    FROM {{ ref('stg_ghg_inventory') }}
    WHERE category IN ('3.5 Waste', '3.2 Construction')
    GROUP BY category, fiscal_year
),

DLC_Emissions AS (
    SELECT
        e.fiscal_year,
        e.category,
        e.total_emission,
        d.dlc_key,
        d.percentage,
        (e.total_emission * d.percentage) AS dlc_emission
    FROM FilteredEmissions AS e
    INNER JOIN {{ source('raw', 'dlc_person_share') }} AS d
        ON 1 = 1  -- Cartesian join to distribute emissions proportionally
),

dlc_mapping AS (
    SELECT
        dlc_key,
        dlc_name,
        MAX(school_area) AS school_area
    FROM {{ source('raw', 'cost_object_warehouse') }}
    WHERE dlc_key IS NOT NULL
    GROUP BY dlc_key, dlc_name
)


SELECT
    e.fiscal_year,
    e.category,
    e.dlc_key,
    e.dlc_emission AS emission,
    d.dlc_name,
    d.school_area
FROM DLC_Emissions AS e
LEFT JOIN dlc_mapping AS d
    ON e.dlc_key = d.dlc_key
WHERE d.dlc_name IS NOT NULL
ORDER BY e.fiscal_year, e.category
