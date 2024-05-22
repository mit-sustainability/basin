from datetime import datetime
import json
import cpi
from dagster import (
    AssetExecutionContext,
    asset,
    AssetIn,
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
from orchestrator.resources.mit_warehouse import MITWHRSResource


logger = get_dagster_logger()


class TravelSpendingData(pa.SchemaModel):
    """Validate the output data schema of travel spending asset"""

    expense_amount: Series[float] = pa.Field(description="Expense Amount")
    expense_type: Series[str] = pa.Field(description="Expense Type")
    trip_end_date: Series[DateTime] = pa.Field(lt="2025", description="Travel Spending Report Date")
    cost_object: Series[int] = pa.Field(ge=0, description="Cost Object ID", coerce=True)
    last_update: Series[DateTime] = pa.Field(description="Date of last update")


def concatenate_csv(unprocessed, s3_client, src_bucket):
    """
    Open csv files to be processed, select relevant columns and rename
    return:: pandas.DataFrame

    """
    dfs = []
    for file_key in unprocessed:
        obj = s3_client.get_object(Bucket=src_bucket, Key=file_key)
        df = pd.read_csv(obj["Body"])
        df["last_update"] = datetime.now()
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


@asset(
    io_manager_key="postgres_append",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(TravelSpendingData),
)
def travel_spending(
    context: AssetExecutionContext,
    s3: S3Resource,
    pg_engine: PostgreConnResources,
) -> pd.DataFrame:
    # this still works because the resource is still available on the context
    source_bucket = "mitos-landing-zone"
    target_table = "travel_spending"

    required_cols = [
        "Expense Amount (reimbursement currency)",
        "Expense Type",
        "Sent for Payment Date",
        "Org Unit 1 - Code",
        "last_update",
    ]
    cols_mapping = {
        required_cols[0]: "expense_amount",
        required_cols[1]: "expense_type",
        required_cols[2]: "trip_end_date",
        required_cols[3]: "cost_object",
    }

    # Get last update from warehouse
    engine = pg_engine.create_engine()
    logger.info("Check last update of the travel_spending table")
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{target_table}')")
        )
        table_exists = result.scalar()
        last_update = datetime(2020, 1, 1, tzinfo=pytz.UTC)
        if table_exists:
            result = conn.execute(text(f"SELECT last_update FROM raw.{target_table}"))
            last_update = pytz.utc.localize(result.scalar())
        conn.commit()
    # Get s3 list
    s3_client = s3.get_client()
    objects = s3_client.list_objects_v2(Bucket=source_bucket)
    unprocessed = [
        obj["Key"]
        for obj in objects.get("Contents", [])
        if (obj["LastModified"] > last_update) and (obj["Key"].endswith(".csv"))
    ]

    # Concatenate and append new rows if there are new entries, select relevant columns
    new_entries_count = 0
    try:
        new_entries = concatenate_csv(unprocessed, s3_client, source_bucket)
        new_entries_count = len(new_entries.index)
    except ValueError:
        logger.info("No new entries found, appending no new entries to the travel_spending table")
        df_out = empty_dataframe_from_model(TravelSpendingData)
    if new_entries_count > 0:
        logger.info(f"Adding {new_entries_count} new entries to the travel_spending table")
        df_out = new_entries[required_cols].rename(columns=cols_mapping).dropna()
        df_out["trip_end_date"] = pd.to_datetime(df_out["trip_end_date"])
        df_out = df_out.sort_values("trip_end_date")
    return df_out


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def annual_cpi_index():
    """Get annual CPI index from python cpi library"""
    DEFAULT_SERIES_ID = cpi.defaults.DEFAULT_SERIES_ID
    logger.info("Ectract the annual CPI data using the python cpi library ")
    cpi_df = cpi.series.get_by_id(DEFAULT_SERIES_ID).to_dataframe()
    cpi_sub = cpi_df[cpi_df["period_code"] == "M13"][["year", "value", "series_id", "series_title"]].sort_values("year")
    cpi_sub["last_update"] = datetime.now()

    return cpi_sub


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def expense_emission_mapper(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the expense_type_to_emissions.json from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Business Travel")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "expense_type_to_emissions.json")
    if download_links is None:
        logger.info("No download links found!")
        return pd.DataFrame()
    response = requests.get(download_links[0], timeout=10)
    if response.status_code == 200:
        payload = json.loads(response.text)
    mapper = {v: key for key, value in payload.items() for v in value}
    df = pd.DataFrame(list(mapper.items()), columns=["expense_type", "emission_category"])
    return df


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def all_scope_summary(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the all_scope summary data from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Business Travel")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "tableau_all_scope.xlsx")
    if download_links is None:
        logger.info("No download links found!")
        return pd.DataFrame()
    # Load the excel file into a pandas dataframe
    df = pd.read_excel(download_links[0], engine="openpyxl")
    return df


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def cost_object_warehouse(dwhrs: MITWHRSResource):
    """This asset ingest cost object table from MIT warehouse"""
    query = (
        "SELECT COST_COLLECTOR_ID, DLC_NAME, SCHOOL_AREA, COST_COLLECTOR_EFFECTIVE_DATE " "FROM WAREUSER.COST_COLLECTOR"
    )
    rows = dwhrs.execute_query(query, chunksize=100000)
    columns = [
        "cost_collector_id",
        "dlc_name",
        "school_area",
        "cost_collector_effective_date",
    ]
    df = pd.DataFrame(rows, columns=columns)
    df["last_update"] = datetime.now()
    n_dlcs, n_schools = df[["dlc_name", "school_area"]].nunique().values
    logger.info(f"The current cost_object table includes {n_dlcs} dlcs and {n_schools} school areas.")
    return df


# Sync processed table back to datahub
dhub_travel_spending = add_dhub_sync(
    asset_name="dhub_travel_spending",
    table_key=["staging", "stg_travel_spending"],
    config={
        "filename": "final_travel_spending.csv",
        "project_name": "Scope3 Business Travel",
        "description": "Travel Spending data with expense group, DLC, emission factors and more",
        "title": "Processed Travel Spending data",
    },
)
