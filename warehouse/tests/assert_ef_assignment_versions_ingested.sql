-- Fails when the assignment seed points at a (family, version) that is not in the archive.
SELECT a.category, a.family, a.version
FROM {{ ref('emission_factor_assignment') }} AS a
LEFT JOIN (SELECT DISTINCT family, version FROM {{ ref('stg_emission_factor_archive') }}) AS e
    ON a.family = e.family AND a.version = e.version
WHERE e.version IS NULL
