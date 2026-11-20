from dagster import define_asset_job
from dagster_dbt import build_dbt_asset_selection

from orchestrator.assets.postgres import mitos_dbt_assets

# Manual job: re-ingest every registered release and rebuild the fiscal-year contract.
# Run it after editing RELEASES or the assignment seed, and before the first nightly dbt build
# in a new environment (the contract models read raw.ef_archive_*).
emission_factor_archive_job = define_asset_job(
    name="emission_factor_archive_job",
    selection=build_dbt_asset_selection([mitos_dbt_assets], dbt_select="emission_factor_by_fiscal_year").upstream(),
)
