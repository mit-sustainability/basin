-- Fails when a stream's weights do not sum to 1, or when a mapped material|disposal key
-- has no factor in some fiscal year the waste category is assigned to.
WITH weights AS (
    SELECT stream, 'weights sum to ' || SUM(weight) AS problem, NULL::INTEGER AS fiscal_year
    FROM {{ ref('waste_stream_factor_map') }}
    GROUP BY stream
    HAVING ABS(SUM(weight) - 1) > 1e-9
),

years AS (
    SELECT DISTINCT fiscal_year FROM {{ ref('emission_factor_by_fiscal_year') }} WHERE category = 'waste'
),

missing AS (
    SELECT m.stream, 'no factor for ' || m.material || '|' || m.disposal AS problem, y.fiscal_year
    FROM {{ ref('waste_stream_factor_map') }} AS m
    CROSS JOIN years AS y
    LEFT JOIN {{ ref('emission_factor_by_fiscal_year') }} AS e
        ON e.category = 'waste'
        AND e.fiscal_year = y.fiscal_year
        AND e.factor_basis = 'activity'
        AND e.factor_key = m.material || '|' || m.disposal
    WHERE e.factor_key IS NULL
)

SELECT * FROM weights
UNION ALL
SELECT * FROM missing
