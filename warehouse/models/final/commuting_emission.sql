WITH combined AS (
    SELECT
        s18."mode",
        s18.mtco2,
        2018 AS "year"
    FROM {{ ref('stg_commuting_survey_2018') }} AS s18
    UNION ALL
    SELECT
        s21."mode",
        s21.mtco2,
        2021 AS "year"
    FROM {{ ref('stg_commuting_survey_2021') }} AS s21
    UNION ALL
    SELECT
        s23."mode",
        s23.mtco2,
        2023 AS "year"
    FROM {{ ref('stg_commuting_survey_2023') }} AS s23
)

SELECT * FROM combined
