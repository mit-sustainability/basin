from dagster import ScheduleDefinition
from dagster_dbt import build_schedule_from_dbt_selection

from orchestrator.assets.postgres import mitos_dbt_assets
from orchestrator.jobs.business_travel_job import business_asset_job
from orchestrator.jobs.website_content_health import website_content_health_job

website_content_health_schedule = ScheduleDefinition(
    job=website_content_health_job,
    cron_schedule="0 9 1 * *",
)

schedules = [
    build_schedule_from_dbt_selection(
        [mitos_dbt_assets],
        job_name="materialize_dbt_models",
        cron_schedule="0 0 * * *",
        dbt_select="fqn:*",
    ),
    ScheduleDefinition(job=business_asset_job, cron_schedule="0 0 1 * *"),
    website_content_health_schedule,
]
