-- set static variables using jinja2 syntax
{% set reply_rate = 0.33 %}
{% set work_week = 50 %}
{% set remote_rate = 0.21 %}

WITH distance AS (
    SELECT
        "drove alone" * {{ var('car_speed') }} * commute_time_average_hours AS drove_alone,
        "carpooled(2-6)"
        * {{ var('car_speed') }}
        * {{ var('car_share_ratio') }}
        * commute_time_average_hours AS carpooled,
        "vanpooled(7+)"
        * {{ var('car_speed') }}
        * {{ var('van_share_ratio') }}
        * commute_time_average_hours AS vanpooled,
        shuttle * {{ var('bus_speed') }} * commute_time_average_hours AS shuttle,
        "public transportation"
        * {{ var('t_ratio') }}
        * {{ var('t_speed') }}
        * commute_time_average_hours AS subway,
        "public transportation"
        * {{ var('rail_ratio') }}
        * {{ var('rail_speed') }}
        * commute_time_average_hours AS commuter_rail,
        "public transportation"
        * {{ var('bus_ratio') }}
        * {{ var('bus_speed') }}
        * commute_time_average_hours AS bus,
        "public transportation"
        * {{ var('intercity_ratio') }}
        * {{ var('intercity_speed') }}
        * commute_time_average_hours AS intercity
    FROM {{ source('raw', 'commuting_survey_2023') }}
),

total_mileage AS (
    SELECT
        SUM(drove_alone) AS drove,
        SUM(subway) AS subway,
        SUM(commuter_rail) AS commuter_rail,
        SUM(intercity) AS intercity,
        SUM(bus) AS bus,
        SUM("carpooled") AS carpooled,
        SUM("vanpooled") AS vanpooled,
        SUM(shuttle) AS shuttle
    FROM distance
),

emission_factors AS (
    SELECT
        vehicle_type,
        "CO2eq_kg",
        ROW_NUMBER() OVER (ORDER BY vehicle_type) AS mode_id
    FROM {{source('raw', 'commuting_emission_factors_EPA')}}
),

-- combine and transpose the mileage CTEs and attach a mode_id
mileage_with_mode AS (
    SELECT
        'drive' AS "mode",
        11 AS mode_id,
        drove AS mile
    FROM total_mileage
    UNION ALL
    SELECT
        'carpooled' AS "mode",
        11 AS mode_id,
        carpooled AS mile
    FROM total_mileage
    UNION ALL
    SELECT
        'vanpooled' AS "mode",
        11 AS mode_id,
        vanpooled AS mile
    FROM total_mileage
    UNION ALL
    SELECT
        'shuttle' AS "mode",
        4 AS mode_id,
        shuttle AS mile
    FROM total_mileage
    UNION ALL
    SELECT
        'subway' AS "mode",
        12 AS mode_id,
        subway AS mile
    FROM total_mileage
    UNION ALL
    SELECT
        'commuter_rail' AS "mode",
        5 AS mode_id,
        commuter_rail AS mile
    FROM total_mileage
    UNION ALL
    SELECT
        'intercity' AS "mode",
        7 AS mode_id,
        intercity AS mile
    FROM total_mileage
    UNION ALL
    SELECT
        'bus' AS "mode",
        4 AS mode_id,
        bus AS mile
    FROM total_mileage
),

attached AS (
    SELECT
        m."mode",
        m.mile,
        ef."CO2eq_kg"
    FROM mileage_with_mode AS m
    LEFT JOIN emission_factors AS ef ON m.mode_id = ef.mode_id

),

-- TO AND FROM, 5 days * 50 working weeks
ghg AS (
    SELECT
        "mode",
        mile,
        mile * "CO2eq_kg" * 2 * 5 * {{work_week}} AS ghg
    FROM attached
),

-- Sum GHG across various public transport methods
transit_ghg AS (
    SELECT
        'public_transportation' AS "mode",
        SUM(g.mile) AS mile,
        SUM(g.ghg) AS ghg
    FROM ghg AS g
    WHERE "mode" IN ('bus', 'commuter_rail', 'intercity', 'subway')

),

-- Scale by survey reply rate and convert unit to metric tons
emission AS (
    SELECT
        g."mode",
        g.mile,
        g.ghg * (1 - {{remote_rate}}) / {{ reply_rate }} / 1000 AS mtco2
    FROM ghg AS g
    WHERE g."mode" IN ('drive', 'vanpooled', 'carpooled', 'shuttle')
    UNION ALL
    SELECT
        t."mode",
        t.mile,
        t.ghg * (1 - {{remote_rate}}) / {{ reply_rate }} / 1000 AS mtco2
    FROM transit_ghg AS t
)

-- Validated, significant change in emission factors from 2018 subway 0.094, intercity 0.06, Bus 0.07, commuter 0.134
SELECT
    "mode",
    mile,
    mtco2
FROM emission
