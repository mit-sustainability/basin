-- set static variables using jinja2 syntax
{% set reply_rate = 0.5 %}
{% set transit_ratio = 0.5 %}
{% set car_share_ratio = 0.4 %}
{% set van_share_ratio = 0.2 %}
{% set work_week = 50 %}

WITH distance AS (
    SELECT
        drive_alone * {{ var('car_speed') }} * commute_time_average_hours AS drive_alone,
        commute_time_average_hours
        * drive_alone_and_public_transport
        * (
            {{ var('car_speed') }} * {{ transit_ratio }}
            + (1 - {{ transit_ratio }}) * {{ var('transit_speed') }}
        ) AS drive_and_public_transport,
        walk_and_public_transport
        * commute_time_average_hours
        * (1 - {{ transit_ratio }})
        * {{ var('transit_speed') }} AS walk_and_public_transport,
        drop_off_and_public_transport
        * commute_time_average_hours
        * (
            {{ var('car_speed') }} * {{ transit_ratio }} * {{ car_share_ratio }}
            + (1 - {{ transit_ratio }}) * {{ var('transit_speed') }}
        ) AS dropoff_and_public_transport,
        bike_and_public_transport
        * commute_time_average_hours
        * (1 - {{ transit_ratio }})
        * {{ var('transit_speed') }} AS bike_and_public_transport,
        "carpooled(2-6)"
        * commute_time_average_hours
        * {{ var('car_speed') }}
        * {{ car_share_ratio }} AS "carpooled(2-6)",
        "vanpooled(7+)"
        * commute_time_average_hours
        * {{ var('car_speed') }}
        * {{ van_share_ratio }} AS "vanpooled(7+)",
        dropped_off
        * commute_time_average_hours
        * {{ var('car_speed') }}
        * {{ van_share_ratio }} AS dropped_off,
        taxi_and_ride_service * {{ var('car_speed') }} * commute_time_average_hours AS taxi
    FROM {{ source('raw', 'commuting_survey_2018') }}
),

daily_mileage AS (
    SELECT
        SUM(drive_alone) AS d,
        SUM(drive_and_public_transport) AS d_pt,
        SUM(walk_and_public_transport) AS w_pt,
        SUM(dropoff_and_public_transport) AS r_pt,
        SUM(bike_and_public_transport) AS b_pt,
        SUM("carpooled(2-6)") AS carpooled,
        SUM("vanpooled(7+)") AS vanpooled,
        SUM(dropped_off) AS dropped_off,
        SUM(taxi) AS taxi
    FROM distance
),

subway_mileage AS (
    SELECT
        d_pt
        * (1 - {{ transit_ratio }})
        * {{var('transit_speed')}}
        / (
            {{ transit_ratio }} * {{var('car_speed')}}
            + (1 - {{ transit_ratio }}) * {{var('transit_speed')}}
        )
        + w_pt
        + r_pt
        * (1 - {{ transit_ratio }})
        * {{var('transit_speed')}}
        / (
            {{ transit_ratio }} * {{var('car_speed')}} * {{car_share_ratio}}
            + (1 - {{ transit_ratio }}) * {{var('transit_speed')}}
        )
        + b_pt AS subway
    FROM daily_mileage
),

transit_mileage AS (
    SELECT
        subway,
        subway AS commuter_rail,
        subway * {{var('rail_speed')}} / {{var('transit_speed')}} AS intercity,
        subway * {{var('bus_speed')}} / {{var('transit_speed')}} AS bus
    FROM subway_mileage
),

mileage AS (
    SELECT
        dm.d
        + dm.d_pt
        * {{ transit_ratio }}
        * {{var('car_speed')}}
        / (
            {{ transit_ratio }} * {{var('car_speed')}}
            + (1 - {{ transit_ratio }}) * {{var('transit_speed')}}
        )
        + dm.r_pt
        * {{ transit_ratio }}
        * {{var('car_speed')}}
        * {{car_share_ratio}}
        / (
            {{ transit_ratio }} * {{var('car_speed')}} * {{car_share_ratio}}
            + (1 - {{ transit_ratio }}) * {{var('transit_speed')}}
        )
        + dm.dropped_off
        + dm.taxi AS drive,
        (t.subway + t.intercity + t.commuter_rail + t.bus) / 4 AS public_transport,
        dm.carpooled,
        dm.vanpooled
    FROM daily_mileage AS dm
    INNER JOIN transit_mileage AS t ON 1 = 1
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
    FROM transit_mileage
    UNION ALL
    SELECT
        'commuter_rail' AS "mode",
        5 AS mode_id,
        commuter_rail AS mile
    FROM transit_mileage
    UNION ALL
    SELECT
        'intercity' AS "mode",
        7 AS mode_id,
        intercity AS mile
    FROM transit_mileage
    UNION ALL
    SELECT
        'bus' AS "mode",
        4 AS mode_id,
        bus AS mile
    FROM transit_mileage
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
        AVG(mile) AS mile,
        AVG(ghg) AS ghg
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
)

-- Validate, significant change in emission factors from 2018 subway 0.094, intercity 0.06, Bus 0.07, commuter 0.134
SELECT
    "mode",
    mile,
    mtco2
FROM scaled
