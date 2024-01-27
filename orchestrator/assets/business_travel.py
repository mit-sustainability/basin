from datetime import datetime
import json
import cpi
from dagster import AssetExecutionContext, asset, get_dagster_logger
from dagster_aws.s3 import S3Resource
from dagster_pandera import pandera_schema_to_dagster_type
import pandas as pd
import pandera as pa
from pandera.typing import Series, DateTime
import pytz
from sqlalchemy import text

from resources.postgres_io_manager import PostgreConnResources


logger = get_dagster_logger()


class TravelSpendingData(pa.SchemaModel):
    """Validate the output data schema of travel spending asset"""

    expense_amount: Series[float] = pa.Field(description="Expense Amount")
    expense_type: Series[str] = pa.Field(description="Expense Type")
    trip_end_date: Series[DateTime] = pa.Field(lt="2025", description="Travel Spending Report Date")
    cost_objects: Series[int] = pa.Field(ge=0, description="Cost Object ID", coerce=True)
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
        required_cols[3]: "cost_objects",
    }

    # Get last update from warehouse
    engine = pg_engine.create_engine()
    logger.info("Check last update of the travel_spending table")
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{target_table}')")
        )
        table_exists = result.scalar()
        tz = pytz.timezone("America/New_York")
        last_update = datetime(2020, 1, 1, tzinfo=tz)
        if table_exists:
            result = conn.execute(text(f"SELECT last_update FROM {target_table}"))
            last_update = result.scalar()
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
    new_entries = concatenate_csv(unprocessed, s3_client, source_bucket)
    new_entries_count = len(new_entries.index)
    df_out = pd.DataFrame()
    if new_entries_count > 0:
        logger.info(f"Adding {new_entries_count} new entries to the travel_spending table")
        df_out = new_entries[required_cols].rename(columns=cols_mapping).dropna()
        df_out["trip_end_date"] = pd.to_datetime(df_out["trip_end_date"])
        df_out = df_out.sort_values("trip_end_date")
        return df_out
    logger.info("New entries found, appending no new entries to the travel_spending table")


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def annual_cpi_index():
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
def cost_object_dlc_mapper(s3: S3Resource):
    ref_bucket = "mitos-resources"
    file_key = "reference/cost_collector_export.csv"
    s3_client = s3.get_client()
    obj = s3_client.get_object(Bucket=ref_bucket, Key=file_key)
    mapper = pd.read_csv(obj["Body"])
    mapper["Cost Object"] = mapper["Cost Object"].dropna().map(lambda x: str(x).replace("S", ""))
    mapper["Cost Object"] = mapper["Cost Object"].fillna(0)
    mapper["Cost Object"] = mapper["Cost Object"].astype("int64")
    mapper.rename(columns={"Cost Object": "cost_object"}, inplace=True)
    return mapper


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def expense_category_mapper(s3: S3Resource):
    ref_bucket = "mitos-resources"
    file_key = "reference/expense_type_to_category.json"
    s3_client = s3.get_client()
    obj = s3_client.get_object(Bucket=ref_bucket, Key=file_key)
    file_content = obj["Body"].read().decode("utf-8")
    payload = json.loads(file_content)
    mapper = {v: key for key, value in payload.items() for v in value}
    df = pd.DataFrame(list(mapper.items()), columns=["type", "category"])
    return df


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def expense_emission_mapper(s3: S3Resource):
    ref_bucket = "mitos-resources"
    file_key = "reference/expense_type_to_emissions.json"
    s3_client = s3.get_client()
    obj = s3_client.get_object(Bucket=ref_bucket, Key=file_key)
    file_content = obj["Body"].read().decode("utf-8")
    payload = json.loads(file_content)
    mapper = {v: key for key, value in payload.items() for v in value}
    df = pd.DataFrame(list(mapper.items()), columns=["expense_type", "emission_category"])
    return df


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def mode_co2_mapper(s3: S3Resource):
    ref_bucket = "mitos-resources"
    file_key = "reference/transport_co2_factors.json"
    s3_client = s3.get_client()
    obj = s3_client.get_object(Bucket=ref_bucket, Key=file_key)
    mapper = pd.read_csv(obj["Body"])
    mapper = mapper.rename(columns={"Transport Mode": "transport_mode", "CO2 Factor": "CO2_factor"})
    return mapper
