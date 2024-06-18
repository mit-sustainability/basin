from dagster import define_asset_job
from dagster_dbt import build_dbt_asset_selection
from orchestrator.assets.postgres import mitos_dbt_assets

dbt_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="stg_purchased_goods_invoice")
pgs_assets = dbt_selection.downstream() | dbt_selection.upstream()

pgs_job = define_asset_job(
    name="purchased_goods_job",
    selection=pgs_assets,
)
