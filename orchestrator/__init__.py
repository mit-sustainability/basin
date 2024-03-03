import os

import boto3
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from orchestrator.resources.postgres_io_manager import (
    PostgreSQLPandasIOManager,
    PostgreConnResources,
)
from dagster_aws.s3 import S3Resource
from dagster_aws.pipes import PipesLambdaClient

from orchestrator.assets.postgres import mitos_dbt_assets
from orchestrator.assets import construction, business_travel

from orchestrator.jobs.business_travel_job import business_asset_job
from orchestrator.jobs.construction_job import construction_asset_job

from orchestrator.constants import (
    dbt_project_dir,
    DWRHS_CREDENTIALS,
    PG_CREDENTIALS,
    dh_api_key,
)
from orchestrator.resources.datahub import DataHubResource
from orchestrator.resources.mit_warehouse import MITWHRSResource
from orchestrator.schedules.mitos_warehouse import schedules

construction_assets = load_assets_from_modules([construction])
business_travel_assets = load_assets_from_modules([business_travel])
defs = Definitions(
    assets=[mitos_dbt_assets] + construction_assets + business_travel_assets,
    schedules=schedules,
    jobs=[business_asset_job, construction_asset_job],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "postgres_replace": PostgreSQLPandasIOManager(**PG_CREDENTIALS),
        "postgres_append": PostgreSQLPandasIOManager(**PG_CREDENTIALS, write_method="append"),
        "pg_engine": PostgreConnResources(**PG_CREDENTIALS),
        "dhub": DataHubResource(auth_token=dh_api_key),
        "dwhrs": MITWHRSResource(**DWRHS_CREDENTIALS),
        "s3": S3Resource(region_name="us-east-1"),
        "lambda_pipes_client": PipesLambdaClient(client=boto3.client("lambda")),
    },
)
