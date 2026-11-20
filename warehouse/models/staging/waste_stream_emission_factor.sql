-- One emission factor per MIT waste stream per fiscal year, collapsed from the EPA Hub
-- material|disposal factors in the emission factor contract via waste_stream_factor_map.

SELECT
    m.stream,
    e.fiscal_year,
    e.version,
    e.unit,
    SUM(m.weight * e.factor_value) AS emission_factor
FROM {{ ref('waste_stream_factor_map') }} AS m
INNER JOIN {{ ref('emission_factor_by_fiscal_year') }} AS e
    ON e.category = 'waste'
    AND e.factor_basis = 'activity'
    AND e.factor_key = m.material || '|' || m.disposal
GROUP BY m.stream, e.fiscal_year, e.version, e.unit
