from dagster import define_asset_job
from dagster_dbt import build_dbt_asset_selection
from orchestrator.assets.postgres import mitos_dbt_assets

dbt_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="construction_expense_emission")
construction_assets = dbt_selection.upstream()

construction_asset_job = define_asset_job(name="construction_asset_job", selection=construction_assets)
