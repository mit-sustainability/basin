from dagster import define_asset_job
from dagster_dbt import build_dbt_asset_selection
from orchestrator.assets.postgres import mitos_dbt_assets

dbt_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="commuting_emission")
commuting_assets = dbt_selection.downstream() | dbt_selection.upstream()

commuting_asset_job = define_asset_job(
    name="commuting_asset_job",
    selection=commuting_assets,
)
