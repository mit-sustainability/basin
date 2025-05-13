from dagster import define_asset_job
from dagster_dbt import build_dbt_asset_selection
from orchestrator.assets.postgres import mitos_dbt_assets

dbt_selection = build_dbt_asset_selection([mitos_dbt_assets], dbt_select="stg_attendance_records")
attendance_assets = dbt_selection.downstream() | dbt_selection.upstream(depth=1)

attendance_job = define_asset_job(
    name="attendance_job",
    selection=attendance_assets,
)
