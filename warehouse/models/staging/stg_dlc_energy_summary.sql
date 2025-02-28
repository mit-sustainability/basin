WITH DLCShare AS (
    SELECT
        building_number,
        dlc_key,
        dlc_share
    FROM {{ ref('stg_building_dlc_share') }}
),

EmissionData AS (
    SELECT
        building_number,
        fiscal_year,
        emission
    FROM {{ ref('stg_building_energy') }}
),

DLC_Emission AS (
    SELECT
        e.fiscal_year,
        d.dlc_key,
        (e.emission * d.dlc_share) AS dlc_emission
    FROM EmissionData AS e
    INNER JOIN DLCShare AS d
        ON e.building_number = d.building_number
),

DLC_Emission_Summary AS (
    SELECT
        fiscal_year,
        dlc_key,
        SUM(dlc_emission) AS total_dlc_emission
    FROM DLC_Emission
    GROUP BY fiscal_year, dlc_key
),

DLC_Mapping AS (
    SELECT
        dlc_key,
        dlc_name,
        MAX(School_Area) AS School_Area
    FROM {{ source('raw', 'cost_object_warehouse') }}
    WHERE dlc_key IS NOT NULL
    GROUP BY dlc_key, dlc_name
)


SELECT
    s.fiscal_year,
    s.dlc_key,
    s.total_dlc_emission AS emission,
    d.dlc_name,
    d.school_area
FROM DLC_Emission_Summary AS s
LEFT JOIN DLC_Mapping AS d
    ON s.dlc_key = d.dlc_key
ORDER BY s.fiscal_year, s.dlc_key
