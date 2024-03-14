"""
To add a daily schedule that materializes your dbt assets, uncomment the following lines.
"""

from dagster import ScheduleDefinition
from dagster_dbt import build_schedule_from_dbt_selection

from orchestrator.assets.postgres import mitos_dbt_assets
from orchestrator.jobs.business_travel_job import business_asset_job

schedules = [
    build_schedule_from_dbt_selection(
        [mitos_dbt_assets],
        job_name="materialize_dbt_models",
        cron_schedule="0 0 * * *",
        dbt_select="fqn:*",
    ),
    ScheduleDefinition(job=business_asset_job, cron_schedule="0 0 1 * *"),
]
