from dagster import define_asset_job
from dagster_dbt import build_dbt_asset_selection
from orchestrator.assets.postgres import mitos_dbt_assets

# Select Material Matters assets
dbt_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="final_waste_recycle")
waste_assets = dbt_selection.upstream()

waste_asset_job = define_asset_job(
    name="waste_asset_job",
    selection=waste_assets,
)

# Select scope 3 Waste assets
waste_scope3_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="final_waste_emission")
waste_emission_assets = waste_scope3_selection.upstream() | waste_scope3_selection.downstream()
waste_emission_job = define_asset_job(
    name="waste_emission_job",
    selection=waste_emission_assets,
)
