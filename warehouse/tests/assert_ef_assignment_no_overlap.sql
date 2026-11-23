-- Fails when two assignment rows of the same (category, family) cover the same fiscal year,
-- including exact duplicates and rows that share a version and fy_start.
WITH assignment AS (
    SELECT *, row_number() OVER () AS row_id
    FROM {{ ref('emission_factor_assignment') }}
)

SELECT a.category, a.family, a.version AS version_a, b.version AS version_b
FROM assignment AS a
JOIN assignment AS b
    ON a.category = b.category
    AND a.family = b.family
    AND a.row_id < b.row_id
WHERE a.fy_start <= COALESCE(b.fy_end, 9999)
  AND b.fy_start <= COALESCE(a.fy_end, 9999)
