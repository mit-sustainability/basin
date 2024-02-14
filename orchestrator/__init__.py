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
    cost_object_warehouse,
    expense_category_mapper,
    expense_emission_mapper,
    mode_co2_mapper,
    all_scope_summary,
    dhub_travel_spending,
)
from orchestrator.assets.construction import (
    emission_factor_USEEIOv2,
    construction_expense,
    dof_maintenance_cost,
)
from orchestrator.constants import (
    dbt_project_dir,
    DWRHS_CREDENTIALS,
    PG_CREDENTIALS,
    dh_api_key,
)
from orchestrator.jobs.business_travel_job import business_asset_job
from orchestrator.resources.datahub import DataHubResource
from orchestrator.resources.mit_warehouse import MITWHRSResource
from orchestrator.schedules.mitos_warehouse import schedules

defs = Definitions(
    assets=[
        mitos_dbt_assets,
        output_test_asset,
        travel_spending,
        annual_cpi_index,
        expense_category_mapper,
        cost_object_warehouse,
        expense_emission_mapper,
        mode_co2_mapper,
        all_scope_summary,
        dhub_travel_spending,
        emission_factor_USEEIOv2,
        construction_expense,
        dof_maintenance_cost,
    ],
    schedules=schedules,
    jobs=[business_asset_job],
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
