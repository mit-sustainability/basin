import os

import boto3
from dagster import Definitions
from dagster_dbt import DbtCliResource
from orchestrator.resources.postgres_io_manager import (
    PostgreSQLPandasIOManager,
    PostgreConnResources,
)
from dagster_aws.s3 import S3Resource
from dagster_aws.pipes import PipesLambdaClient

from orchestrator.assets.postgres import (
    mitos_dbt_assets,
    output_test_asset,
)
from orchestrator.assets.business_travel import (
    travel_spending,
    annual_cpi_index,
    cost_object_dlc_mapper,
    expense_category_mapper,
    expense_emission_mapper,
    mode_co2_mapper,
)
from orchestrator.constants import dbt_project_dir, PG_CREDENTIALS
from orchestrator.schedules.mitos_warehouse import schedules
from orchestrator.jobs.business_travel_job import business_asset_job


defs = Definitions(
    assets=[
        mitos_dbt_assets,
        output_test_asset,
        travel_spending,
        annual_cpi_index,
        expense_category_mapper,
        cost_object_dlc_mapper,
        expense_emission_mapper,
        mode_co2_mapper,
    ],
    schedules=schedules,
    jobs=[business_asset_job],
    resources={
        "dbt": DbtCliResource(project_dir=os.fspath(dbt_project_dir)),
        "postgres_pandas_io": PostgreSQLPandasIOManager(**PG_CREDENTIALS),
        "pg_engine": PostgreConnResources(**PG_CREDENTIALS),
        "s3": S3Resource(region_name="us-east-1"),
        "lambda_pipes_client": PipesLambdaClient(client=boto3.client("lambda")),
    },
)
