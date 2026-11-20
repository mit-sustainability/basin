-- Fails (returns rows) when inflate_to_dollar_year() disagrees with CPI ratios computed directly.
WITH cpi AS (
    SELECT year::INTEGER AS year, value::FLOAT AS value FROM {{ source('raw', 'annual_cpi_index') }}
),

latest AS (
    SELECT value FROM cpi ORDER BY year DESC LIMIT 1
),

cases (amount, spend_year, dollar_year, expected) AS (
    SELECT 100.0, 2021, 2021, 100.0
    UNION ALL
    SELECT 100.0, 2012, 2021,
        100.0 * (SELECT value FROM cpi WHERE year = 2021) / (SELECT value FROM cpi WHERE year = 2012)
    UNION ALL
    -- missing spend year falls back to the latest CPI
    SELECT 100.0, 3000, 2021, 100.0 * (SELECT value FROM cpi WHERE year = 2021) / (SELECT value FROM latest)
    UNION ALL
    -- missing target dollar year yields NULL, never a silently different year
    SELECT 100.0, 2021, 3000, NULL
),

calc AS (
    SELECT
        *,
        {{ inflate_to_dollar_year('amount', 'spend_year', 'dollar_year') }} AS got
    FROM cases
)

SELECT *
FROM calc
WHERE (got IS NULL) <> (expected IS NULL)
    OR ABS(got - expected) > 1e-9
