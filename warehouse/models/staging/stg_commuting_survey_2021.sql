-- set static variables using jinja2 syntax
{% set reply_rate = 0.45 %}
{% set work_week = 50 %}

WITH overall_time AS (
    SELECT *
    FROM {{ source('raw', 'commuting_survey_2021') }}
),

total_time AS (
    SELECT
        SUM("drove alone") AS drove_alone,
        SUM("carpooled(2-6)") AS carpooled,
        SUM("vanpooled(7+)") AS vanpooled,
        SUM(shuttle) AS shuttle,
        SUM("public transportation") AS transit
    FROM overall_time
),

emission_factors AS (
    SELECT
        vehicle_type,
        "CO2eq_kg",
        ROW_NUMBER() OVER (ORDER BY vehicle_type) AS mode_id
    FROM {{source('raw', 'commuting_emission_factors_EPA')}}
),

duration_with_mode AS (
    SELECT
        'drive' AS "mode",
        11 AS mode_id,
        drove_alone AS duration,
        {{ var('car_speed') }} AS speed_factor
    FROM total_time
    UNION ALL
    SELECT
        'carpooled' AS "mode",
        11 AS mode_id,
        carpooled * {{ var('car_share_ratio') }} AS duration,
        {{ var('car_speed') }} AS speed_factor
    FROM total_time
    UNION ALL
    SELECT
        'vanpooled' AS "mode",
        11 AS mode_id,
        vanpooled * {{ var('van_share_ratio') }} AS duration,
        {{ var('car_speed') }} AS speed_factor
    FROM total_time
    UNION ALL
    SELECT
        'shuttle' AS "mode",
        4 AS mode_id,
        shuttle AS duration, -- shuttle emission factor is per passenger mile
        {{ var('bus_speed') }} AS speed_factor
    FROM total_time
    UNION ALL
    SELECT
        'subway' AS "mode",
        12 AS mode_id,
        transit * {{ var('t_ratio') }} AS duration,
        {{ var('t_speed') }} AS speed_factor
    FROM total_time
    UNION ALL
    SELECT
        'commuter_rail' AS "mode",
        5 AS mode_id,
        transit * {{ var('rail_ratio') }} AS duration,
        {{ var('rail_speed') }} AS speed_factor
    FROM total_time
    UNION ALL
    SELECT
        'intercity' AS "mode",
        7 AS mode_id,
        transit * {{ var('intercity_ratio') }} AS duration,
        {{ var('intercity_speed') }} AS speed_factor
    FROM total_time
    UNION ALL
    SELECT
        'bus' AS "mode",
        4 AS mode_id,
        transit * {{ var('bus_ratio') }} AS duration,
        {{ var('bus_speed') }} AS speed_factor
    FROM total_time
),


attached AS (
    SELECT
        m."mode",
        m.duration,
        ef."CO2eq_kg",
        m.speed_factor
    FROM duration_with_mode AS m
    LEFT JOIN emission_factors AS ef ON m.mode_id = ef.mode_id

),

ghg AS (
    SELECT
        "mode",
        duration,
        duration / 60 * "CO2eq_kg" * speed_factor AS ghg -- duration in hour * emission factor * speed factor
    FROM attached
),

public_transit_ghg AS (
    SELECT
        'public_transportation' AS "mode",
        SUM(ghg) AS ghg
    FROM ghg
    WHERE "mode" IN ('bus', 'intercity', 'subway', 'commuter_rail')

),

-- TO AND FROM / reply rate / 1000 FOR unit CONVERSION
emission AS (
    SELECT
        "mode",
        ghg * 2 * {{work_week}} / {{reply_rate}} / 1000 AS mtco2
    FROM ghg
    WHERE "mode" IN ('drive', 'carpooled', 'vanpooled', 'shuttle')
    UNION ALL
    SELECT
        "mode",
        ghg * 2 * {{work_week}} / {{reply_rate}} / 1000 AS mtco2
    FROM public_transit_ghg
),

-- Mode share
ms AS (
    SELECT
        year,
        "mode",
        count,
        (count / SUM(count) OVER (PARTITION BY year)) AS share
    FROM
        {{ source('raw', 'commuting_survey_modes') }}
    WHERE year = 2021
    ORDER BY
        "mode"
)

-- Validate, significant change in emission factors from 2018 subway 0.094, intercity 0.06, Bus 0.07, commuter 0.134
SELECT
    m."mode",
    m."share",
    COALESCE(e.mtco2, 0) AS mtco2
FROM ms AS m
LEFT JOIN emission AS e ON m."mode" = e."mode"
