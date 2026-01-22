WITH cat AS (
    SELECT
        *,
        CASE
            WHEN district_electricity = TRUE THEN 'district'
            WHEN district_electricity = FALSE THEN 'standalone'
            ELSE building_group
        END AS category
    FROM {{ source('staging', 'stg_utility_history') }}
    WHERE building_group = 'main'
),


distinct_building_areas AS (
    SELECT DISTINCT
        fiscal_year,
        cost_collector_name, -- or cost_collector_id to be safer
        ext_gross_area,
        category
    FROM cat
),


ext_area_fy AS (
    SELECT
        fiscal_year,
        category,
        SUM(ext_gross_area) AS total_gross_area
    FROM distinct_building_areas
    GROUP BY
        fiscal_year,
        category
    ORDER BY
        fiscal_year,
        category
),

ut_total AS (
    SELECT
        fiscal_year,
        utility_type,
        category,
        SUM(utility_cost) AS total_cost,
        SUM(utility_usage) AS total_usage,
        SUM(utility_mmbtu) AS total_mmbtu
    FROM cat
    GROUP BY fiscal_year, utility_type, category
    ORDER BY fiscal_year, utility_type, category
)


SELECT
    t.*,
    e.total_gross_area
FROM ut_total AS t
LEFT JOIN ext_area_fy AS e
    ON
        t.fiscal_year = e.fiscal_year
        AND t.category = e.category
