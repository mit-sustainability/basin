-- Fails when two assignment rows of the same (category, family) cover the same fiscal year.
SELECT a.category, a.family, a.version AS version_a, b.version AS version_b
FROM {{ ref('emission_factor_assignment') }} AS a
JOIN {{ ref('emission_factor_assignment') }} AS b
    ON a.category = b.category
    AND a.family = b.family
    AND (a.version, a.fy_start) < (b.version, b.fy_start)
WHERE a.fy_start <= COALESCE(b.fy_end, 9999)
  AND b.fy_start <= COALESCE(a.fy_end, 9999)
