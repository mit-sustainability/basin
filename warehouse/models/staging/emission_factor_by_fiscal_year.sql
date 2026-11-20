{{ config(materialized='table') }}

-- Emission factor contract: one row per (category, fiscal_year, factor_key, factor_basis).
-- Open-ended assignments run through next fiscal year, so a new run always has factors.

WITH assignment AS (
    SELECT
        category,
        family,
        version,
        generate_series(
            fy_start,
            COALESCE(fy_end, ({{ fiscal_year('current_date') }})::INTEGER + 1)
        ) AS fiscal_year
    FROM {{ ref('emission_factor_assignment') }}
)

SELECT
    a.category,
    a.fiscal_year,
    e.factor_key,
    e.factor_key_type,
    e.factor_basis,
    e.family,
    e.version,
    e.publisher,
    e.factor_value,
    e.unit,
    e.dollar_year,
    e.gwp_basis
FROM assignment AS a
INNER JOIN {{ ref('stg_emission_factor_archive') }} AS e
    ON a.family = e.family
    AND a.version = e.version
