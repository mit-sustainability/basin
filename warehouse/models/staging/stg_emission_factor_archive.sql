-- Every archived emission factor release in one long table.
-- Raw tables share one schema (orchestrator/assets/emission_factors.py ARCHIVE_COLUMNS).

{% set archive_tables = [
    'ef_archive_supply_chain',
    'ef_archive_cornerstone_eeio',
    'ef_archive_ghg_ef_hub',
    'ef_archive_simap_food',
] %}

{% for t in archive_tables %}
SELECT
    family,
    version,
    publisher,
    factor_key,
    factor_key_type,
    factor_basis,
    factor_name,
    factor_value,
    unit,
    CASE WHEN factor_basis = 'spend' THEN dollar_year::INTEGER END AS dollar_year,
    gwp_basis
FROM {{ source('raw', t) }}
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
