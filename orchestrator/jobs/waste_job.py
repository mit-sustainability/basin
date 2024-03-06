from dagster import define_asset_job
from dagster_dbt import build_dbt_asset_selection
from orchestrator.assets.postgres import mitos_dbt_assets

# dbt_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="construction_expense_emission")
# construction_assets = dbt_selection.upstream()

waste_asset_job = define_asset_job(
    name="waste_asset_job",
    selection=[
        "small_stream_recycle",
        "newbatch_waste_recycle",
        "historical_waste_recycle",
    ],
)
