from datetime import datetime
from dagster import (
    Output,
    asset,
    get_dagster_logger,
    MetadataValue,
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

from orchestrator.assets.utils import (
    empty_dataframe_from_model,
    add_dhub_sync,
    normalize_column_name,
)
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


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    # dagster_type=pandera_schema_to_dagster_type(TravelSpendingData),
)
def purchased_energy(em_connect: PostgreConnResources) -> Output[pd.DataFrame]:
    engine = em_connect.create_engine()
    logger.info("Connect to energize_mit database to ingest Scope 1 and 2 emissions")
    query = """
            SELECT
                "GL_ACCOUNT_KEY",
                "START_DATE",
                "START_DATE_USE",
                "BILLING_FY",
                "USE_FY",
                "GHG",
                "UNIT_OF_MEASURE",
                "NUMBER_OF_UNITS",
                "BUILDING_GROUP_LEVEL1",
                "LEVEL1_CATEGORY",
                "LEVEL2_CATEGORY",
                "LEVEL3_CATEGORY"
            FROM public.energy_cdr
            WHERE "BUILDING_GROUP_LEVEL1" IN ('CUP','Non-CUP')
            AND "LEVEL2_CATEGORY" = 'Purchased'
            AND "LEVEL3_CATEGORY" IN ('Electricity', 'Gas', 'Fuel Oil #2', 'Fuel Oil #6')
            """
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
        logger.info("Query executed successfully. Scope 1 and 2 data fetched from energize-mit.")
    except Exception as e:
        logger.error("An error occurred:", e)
    finally:
        engine.dispose()

    metadata = {
        "total_entries": len(df),
        "last_entry_date": MetadataValue.text(df["START_DATE"].max().strftime("%Y-%m-%d")),
    }
    df["last_update"] = datetime.now()
    df.columns = [normalize_column_name(col) for col in df.columns]
    return Output(value=df, metadata=metadata)


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
