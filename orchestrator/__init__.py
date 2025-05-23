import os

import boto3
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from orchestrator.resources.postgres_io_manager import (
    PostgreSQLPandasIOManager,
    PostgreConnResources,
)
from dagster_aws.pipes import PipesLambdaClient
from dagster_aws.s3 import S3Resource

from orchestrator.assets.postgres import mitos_dbt_assets
from orchestrator.assets import (
    business_travel,
    commuting,
    construction,
    food,
    parking,
    purchased_goods,
    waste,
    ghg_inventory,
    ghg_footprint,
    campus_facility,
    engagement,
)

from orchestrator.jobs.business_travel_job import business_asset_job
from orchestrator.jobs.construction_job import construction_asset_job
from orchestrator.jobs.commuting_job import commuting_asset_job
from orchestrator.jobs.food_job import food_asset_job
from orchestrator.jobs.ghg_inventory import ghg_job
from orchestrator.jobs.parking_job import parking_asset_job
from orchestrator.jobs.purchased_goods import pgs_job
from orchestrator.jobs.waste_job import waste_asset_job
from orchestrator.jobs.dlc_footprint import footprint_job
from orchestrator.jobs.engagement import attendance_job
from orchestrator.constants import (
    dbt_project_dir,
    DWRHS_CREDENTIALS,
    PG_CREDENTIALS,
    EM_CREDENTIALS,
    dh_api_key,
)
from orchestrator.resources.datahub import DataHubResource
from orchestrator.resources.mit_warehouse import MITWHRSResource
from orchestrator.schedules.mitos_warehouse import schedules
from orchestrator.sensors.s3_bucket import sensor_ghg_manual

### TODO: Set a utility function folders, and implement recursive search for jobs and schedules.
# https://github.com/dagster-io/dagster/issues/12359

construction_assets = load_assets_from_modules([construction])
business_travel_assets = load_assets_from_modules([business_travel])
waste_assets = load_assets_from_modules([waste])
commuting_assets = load_assets_from_modules([commuting])
parking_assets = load_assets_from_modules([parking])
purchased_goods_assets = load_assets_from_modules([purchased_goods])
food_assets = load_assets_from_modules([food])
all_scopes_assets = load_assets_from_modules([ghg_inventory])
footprint_assets = load_assets_from_modules([ghg_footprint])
campus_facility_assets = load_assets_from_modules([campus_facility])
engagement_assets = load_assets_from_modules([engagement])

defs = Definitions(
    assets=[mitos_dbt_assets]
    + construction_assets
    + business_travel_assets
    + waste_assets
    + commuting_assets
    + parking_assets
    + purchased_goods_assets
    + food_assets
    + all_scopes_assets
    + footprint_assets
    + campus_facility_assets
    + engagement_assets,
    schedules=schedules,
    jobs=[
        business_asset_job,
        construction_asset_job,
        waste_asset_job,
        commuting_asset_job,
        parking_asset_job,
        pgs_job,
        food_asset_job,
        ghg_job,
        footprint_job,
        attendance_job,
    ],
    sensors=[sensor_ghg_manual],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "postgres_replace": PostgreSQLPandasIOManager(**PG_CREDENTIALS),
        "postgres_append": PostgreSQLPandasIOManager(**PG_CREDENTIALS, write_method="append"),
        "pg_engine": PostgreConnResources(**PG_CREDENTIALS),
        "em_connect": PostgreConnResources(**EM_CREDENTIALS),
        "dhub": DataHubResource(auth_token=dh_api_key),
        "dwrhs": MITWHRSResource(**DWRHS_CREDENTIALS),
        "s3": S3Resource(region_name="us-east-1"),
        "lambda_pipes_client": PipesLambdaClient(client=boto3.client("lambda")),
    },
)
