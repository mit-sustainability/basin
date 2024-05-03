-- set static variables using jinja2 syntax
{% set reply_rate = 0.49 %}
{% set transit_ratio = 0.8 %}
{% set work_week = 50 %}



WITH binned_count AS (
    SELECT
        drive_alone
        + (1 - {{ transit_ratio }}) * drive_alone_and_public_transport
        + taxi_and_ride_service
        + dropped_off
        + (1 - {{ transit_ratio }}) * drop_off_and_public_transport AS drive,
        (
            drive_alone_and_public_transport
            + walk_and_public_transport
            + bike_and_public_transport
            + drop_off_and_public_transport
        )
        * {{ transit_ratio }} AS public_transportation,
        "carpooled(2-6)" AS "carpooled(2-6)",
        "vanpooled(7+)",
        commute_time_average_hours
    FROM {{ source('raw', 'commuting_survey_2018') }}
),


distance AS (
    SELECT
        drive * {{ var('car_speed') }} * commute_time_average_hours AS drive,
        "carpooled(2-6)"
        * commute_time_average_hours
        * {{ var('car_speed') }}
        * {{ var("car_share_ratio") }} AS "carpooled(2-6)",
        "vanpooled(7+)"
        * commute_time_average_hours
        * {{ var('car_speed') }}
        * {{ var("van_share_ratio") }} AS "vanpooled(7+)",
        "public_transportation"
        * {{ var('t_ratio') }}
        * {{ var('t_speed') }}
        * commute_time_average_hours AS subway,
        "public_transportation"
        * {{ var('rail_ratio') }}
        * {{ var('rail_speed') }}
        * commute_time_average_hours AS commuter_rail,
        "public_transportation"
        * {{ var('bus_ratio') }}
        * {{ var('bus_speed') }}
        * commute_time_average_hours AS bus,
        "public_transportation"
        * {{ var('intercity_ratio') }}
        * {{ var('intercity_speed') }}
        * commute_time_average_hours AS intercity
    FROM binned_count
),

mileage AS (
    SELECT
        SUM(drive) AS drive,
        SUM("carpooled(2-6)") AS carpooled,
        SUM("vanpooled(7+)") AS vanpooled,
        SUM(subway) AS subway,
        SUM(commuter_rail) AS commuter_rail,
        SUM(bus) AS bus,
        SUM(intercity) AS intercity
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
        drive AS mile
    FROM mileage
    UNION ALL
    SELECT
        'carpooled' AS "mode",
        11 AS mode_id,
        carpooled AS mile
    FROM mileage
    UNION ALL
    SELECT
        'vanpooled' AS "mode",
        11 AS mode_id,
        vanpooled AS mile
    FROM mileage
    UNION ALL
    SELECT
        'subway' AS "mode",
        12 AS mode_id,
        subway AS mile
    FROM mileage
    UNION ALL
    SELECT
        'commuter_rail' AS "mode",
        5 AS mode_id,
        commuter_rail AS mile
    FROM mileage
    UNION ALL
    SELECT
        'intercity' AS "mode",
        7 AS mode_id,
        intercity AS mile
    FROM mileage
    UNION ALL
    SELECT
        'bus' AS "mode",
        4 AS mode_id,
        bus AS mile
    FROM mileage
),

attached AS (
    SELECT
        m."mode",
        m.mile,
        ef."CO2eq_kg"
    FROM mileage_with_mode AS m
    LEFT JOIN emission_factors AS ef ON m.mode_id = ef.mode_id

),

emission AS (
    SELECT
        "mode",
        mile,
        mile * "CO2eq_kg" * 2 * 5 * {{work_week}} AS ghg -- TO AND FROM, 5 days a week, 50 working weeks
    FROM attached
),

-- Average GHG across various public transport methods
transit_ghg AS (
    SELECT
        'public_transportation' AS "mode",
        SUM(mile) AS mile,
        SUM(ghg) AS ghg
    FROM emission
    WHERE "mode" IN ('bus', 'commuter_rail', 'intercity', 'subway')

),

-- Scale by survey reply rate and convert unit to metric tons
scaled AS (
    SELECT
        e."mode",
        e.mile,
        e.ghg / {{ reply_rate }} / 1000 AS mtco2
    FROM emission AS e
    WHERE e."mode" IN ('drive', 'vanpooled', 'carpooled')
    UNION ALL
    SELECT
        t."mode",
        t.mile,
        t.ghg / {{ reply_rate }} / 1000 AS mtco2
    FROM transit_ghg AS t
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
    WHERE year = 2018
    ORDER BY
        "mode"
)

-- Validate, significant change in emission factors from 2018 subway 0.094, intercity 0.06, Bus 0.07, commuter 0.134
SELECT
    m."mode",
    m."share",
    COALESCE(s.mile, 0) AS mile,
    COALESCE(s.mtco2, 0) AS mtco2
FROM ms AS m
LEFT JOIN scaled AS s ON m."mode" = s."mode"
