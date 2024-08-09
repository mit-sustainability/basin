from datetime import datetime
import json
import cpi
from dagster import (
    Output,
    asset,
    get_dagster_logger,
    ResourceParam,
)
from dagster_aws.s3 import S3Resource
from dagster_pandera import pandera_schema_to_dagster_type
import pandas as pd
import pandera as pa
from pandera.typing import Series, DateTime
import pytz
import requests
from sqlalchemy import text

from orchestrator.assets.utils import empty_dataframe_from_model, add_dhub_sync
from orchestrator.resources.postgres_io_manager import PostgreConnResources
from orchestrator.resources.datahub import DataHubResource


logger = get_dagster_logger()


# class TravelSpendingData(pa.SchemaModel):
#     """Validate the output data schema of travel spending asset"""

#     expense_amount: Series[float] = pa.Field(description="Expense Amount")
#     expense_type: Series[str] = pa.Field(description="Expense Type")
#     trip_end_date: Series[DateTime] = pa.Field(
#         lt="2025", description="Travel Spending Report Date"
#     )
#     cost_object: Series[int] = pa.Field(ge=0, description="Cost Object ID", coerce=True)
#     last_update: Series[DateTime] = pa.Field(description="Date of last update")


@asset(
    io_manager_key="postgres_append",
    compute_kind="python",
    group_name="raw",
    # dagster_type=pandera_schema_to_dagster_type(TravelSpendingData),
)
def ghg_manual_entries(s3: S3Resource) -> Output[pd.DataFrame]:
    source_bucket = "mitos-landing-zone"
    # Get the data from S3
    s3_client = s3.get_client()
    obj = s3_client.get_object(Bucket=source_bucket, Key="all-scopes-sync/quickbase_data.csv")
    df = pd.read_csv(obj["Body"])

    metadata = {
        "total_entries": len(df),
    }
    sel_columns = ["category", "emission", "fiscal_year", "scope", "Date Modified"]
    df_out = df[sel_columns]
    df_out.rename({"Date Modified": "last_update"}, axis=1, inplace=True)

    return Output(value=df_out, metadata=metadata)


# Sync processed table back to datahub
# dhub_travel_spending = add_dhub_sync(
#     asset_name="dhub_travel_spending",
#     table_key=["staging", "stg_travel_spending"],
#     config={
#         "filename": "final_travel_spending.csv",
#         "project_name": "Scope3 Business Travel",
#         "description": "Travel Spending data with expense group, DLC, emission factors and more",
#         "title": "Processed Travel Spending data",
#     },
# )
