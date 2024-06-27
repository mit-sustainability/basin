from datetime import datetime

from dagster import (
    asset,
    AssetIn,
    get_dagster_logger,
    ResourceParam,
)
from dagster_pandera import pandera_schema_to_dagster_type
import pandas as pd
import pandera as pa
from pandera.typing import Series, DateTime
from prophet import Prophet

from orchestrator.assets.utils import add_dhub_sync
from orchestrator.resources.datahub import DataHubResource
from orchestrator.resources.mit_warehouse import MITWHRSResource

logger = get_dagster_logger()

sel_columns = [
    "date",
    "unique_1016",
    "unique_1018",
    "unique_1022",
    "unique_1024",
    "unique_1025",
    "unique_1030",
    "unique_1035",
    "unique_1037",
    "unique_1038",
    "total_unique",
]


class ParkingNewbatchData(pa.SchemaModel):
    """Validate the output data schema of newbatch parking data"""

    date: Series[DateTime] = pa.Field(description="Waste collection building id")
    parking_lot: Series[str] = pa.Field(description="Waste collection building name")
    total: Series[int] = pa.Field(description="Waste collection site street")
    unique: Series[int] = pa.Field(description="Service Date")
    last_update: Series[DateTime] = pa.Field(description="Date of last update")


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def historical_parking_daily(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the daily historical parking data from the Data Hub"""
    project_id = dhub.get_project_id("Parking")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "historical_parking_daily")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    df = pd.read_csv(download_links[0])
    df["date"] = pd.to_datetime(df["date"])
    logger.info(f"Loaded historical parking data till: {df.date.max()}!")
    return df[sel_columns]


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(ParkingNewbatchData),
)
def newbatch_parking_daily(dwhrs: MITWHRSResource):
    """This asset ingest the new batch of parking activity data from MIT Warehouse"""
    query = """
            SELECT ENTRY_DATE AS "date",
                PARKING_LOT_ID AS parking_lot,
                COUNT(PARKER_ACCOUNT_ID) AS "total",
                COUNT(DISTINCT MIT_ID) AS "unique"
            FROM WAREUSER.PARK_TX_ACTIVITY
            WHERE (PARK_ACTIVITY_TYPE_KEY = 'G' OR PARK_ACTIVITY_TYPE_KEY = 'E')
                AND ENTRY_DATE > TO_DATE('2021-06-25', 'YYYY-MM-DD')
                AND TO_CHAR(ENTRY_DATE, 'DY', 'NLS_DATE_LANGUAGE = AMERICAN') NOT IN ('SAT', 'SUN')
            GROUP BY ENTRY_DATE, PARKING_LOT_ID
            """
    rows = dwhrs.execute_query(query, chunksize=100000)
    columns = [
        "date",
        "parking_lot",
        "total",
        "unique",
    ]
    df = pd.DataFrame(rows, columns=columns)
    df["last_update"] = datetime.now()
    logger.info(f"Last updated batch parking transaction data since date: {df.date.min()}")
    return df


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def mit_holidays(dwhrs: MITWHRSResource):
    """This asset ingest the MIT holidays data from MIT Warehouse"""
    query = """
            SELECT HOLIDAY_CLOSING_DATE AS "date",
                HOLIDAY_CLOSING_DESCRIPTION AS holiday,
                HOLIDAY_CLOSING_TYPE AS holiday_type
            FROM WAREUSER.MIT_HOLIDAY_CLOSING_CALENDAR
            WHERE HOLIDAY_CLOSING_TYPE IN ('Standard Holiday', 'Special Holiday/Closing')
                AND HOLIDAY_CLOSING_DATE > TO_DATE('2015-09-01', 'YYYY-MM-DD')
                AND HOLIDAY_CLOSING_DATE < SYSDATE
            ORDER BY HOLIDAY_CLOSING_DATE
            """
    rows = dwhrs.execute_query(query, chunksize=100000)
    columns = [
        "date",
        "holiday",
        "holiday_type",
    ]
    df = pd.DataFrame(rows, columns=columns)
    df["last_update"] = datetime.now()
    logger.info(f"Last updated of MIT holidays: {df.last_update.max()}")
    return df


@asset(
    compute_kind="python",
    group_name="final",
    key_prefix="final",
    io_manager_key="postgres_replace",
    ins={
        "df": AssetIn(key=["staging", "stg_parking_daily"], input_manager_key="postgres_replace"),
        "holidays": AssetIn(key=["mit_holidays"], input_manager_key="postgres_replace"),
    },
)
def daily_parking_trend(df: pd.DataFrame, holidays: pd.DataFrame):
    """This asset takes in the combined parking data and MIT holidays table
    to forecast the parking trend"""
    df["ds"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None)
    filtered = df[df["ds"].dt.dayofweek < 5]
    filtered["ds"] = filtered["ds"].dt.normalize()
    holidays["ds"] = pd.to_datetime(holidays["date"], utc=True).dt.tz_localize(None)
    logger.info("Fit a Prophet model using total_unique")
    ts = filtered[["ds", "total_unique"]].rename(columns={"total_unique": "y"})
    m = Prophet(
        holidays=holidays,
        daily_seasonality=False,
        changepoint_range=0.99,
        n_changepoints=500,
        changepoint_prior_scale=0.4,
    )
    m.fit(ts)
    all_time = pd.DataFrame({"ds": pd.date_range(start="2015-09-15", end="2024-01-01", freq="D")})
    prediction = m.predict(all_time)
    df_out = pd.merge(filtered, prediction, on="ds", how="left")
    logger.info(f"Successfully merge and predict parking trends till {df_out.ds.max()}")
    out_cols = sel_columns.copy()
    out_cols.append("trend")
    return df_out[out_cols]


dhub_waste_sync = add_dhub_sync(
    asset_name="dhub_parking_daily",
    table_key=["final", "daily_parking_trend"],
    config={
        "filename": "daily_parking_trend",
        "project_name": "Parking",
        "description": "Daily Parking activity data with forecasted trends",
        "title": "Daily Parking activity data since 2015",
    },
)
