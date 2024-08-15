from dagster import define_asset_job
from dagster_dbt import build_dbt_asset_selection
from orchestrator.assets.postgres import mitos_dbt_assets

# Select Material Matters assets
dbt_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="final_waste_emission")
waste_assets = dbt_selection.upstream() | dbt_selection.downstream()

waste_asset_job = define_asset_job(
    name="waste_asset_job",
    selection=waste_assets,
)
