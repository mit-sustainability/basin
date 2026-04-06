from datetime import datetime

from dagster import (
    asset,
    AssetIn,
    Config,
    Failure,
    get_dagster_logger,
    MetadataValue,
    Output,
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

DEFAULT_PARKING_QUERY_START_DATE = "2021-06-25"
DEFAULT_PARKING_PREDICTION_START_DATE = "2015-09-15"
DEFAULT_PARKING_PREDICTION_END_DATE = datetime.today().strftime("%Y-%m-%d")

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


class ParkingNewbatchData(pa.DataFrameModel):
    """Validate the output data schema of newbatch parking data"""

    date: Series[DateTime] = pa.Field(description="Waste collection building id")
    parking_lot: Series[str] = pa.Field(description="Waste collection building name")
    total: Series[int] = pa.Field(description="Waste collection site street")
    unique: Series[int] = pa.Field(description="Service Date")
    last_update: Series[DateTime] = pa.Field(description="Date of last update")


class ParkingNewbatchConfig(Config):
    """Configuration for the incremental MIT warehouse parking query."""

    start_date: str = DEFAULT_PARKING_QUERY_START_DATE
    end_date: str | None = None
    exclude_weekends: bool = True


class ParkingTrendConfig(Config):
    """Configuration for the parking trend prediction horizon."""

    prediction_end_date: str = DEFAULT_PARKING_PREDICTION_END_DATE


def _normalize_config_date(value: str, *, field_name: str) -> str:
    try:
        return pd.to_datetime(value, format="%Y-%m-%d", errors="raise").strftime("%Y-%m-%d")
    except (TypeError, ValueError) as exc:
        raise Failure(f"`{field_name}` must be a valid date in YYYY-MM-DD format.") from exc


def _resolve_parking_query_dates(
    start_date: str,
    end_date: str | None,
) -> tuple[str, str | None]:
    normalized_start_date = _normalize_config_date(start_date, field_name="start_date")
    normalized_end_date = _normalize_config_date(end_date, field_name="end_date") if end_date is not None else None
    if normalized_end_date is not None and normalized_end_date < normalized_start_date:
        raise Failure("Parking query date range is invalid: `end_date` must be on or after `start_date`.")
    return normalized_start_date, normalized_end_date


def _build_newbatch_parking_query(config: ParkingNewbatchConfig) -> str:
    start_date, end_date = _resolve_parking_query_dates(config.start_date, config.end_date)
    filters = [
        "(PARK_ACTIVITY_TYPE_KEY = 'G' OR PARK_ACTIVITY_TYPE_KEY = 'E')",
        f"ENTRY_DATE >= TO_DATE('{start_date}', 'YYYY-MM-DD')",
    ]
    if end_date is not None:
        filters.append(f"ENTRY_DATE <= TO_DATE('{end_date}', 'YYYY-MM-DD')")
    if config.exclude_weekends:
        filters.append("TO_CHAR(ENTRY_DATE, 'DY', 'NLS_DATE_LANGUAGE = AMERICAN') NOT IN ('SAT', 'SUN')")

    where_clause = "\n                AND ".join(filters)
    return f"""
            SELECT ENTRY_DATE AS "date",
                PARKING_LOT_ID AS parking_lot,
                COUNT(PARKER_ACCOUNT_ID) AS "total",
                COUNT(DISTINCT MIT_ID) AS "unique"
            FROM WAREUSER.PARK_TX_ACTIVITY
            WHERE {where_clause}
            GROUP BY ENTRY_DATE, PARKING_LOT_ID
            """


def _build_parking_prediction_range(prediction_end_date: str) -> pd.DataFrame:
    normalized_end_date = _normalize_config_date(
        prediction_end_date,
        field_name="prediction_end_date",
    )
    if normalized_end_date < DEFAULT_PARKING_PREDICTION_START_DATE:
        raise Failure(
            "Parking prediction range is invalid: `prediction_end_date` must be on or after "
            f"`{DEFAULT_PARKING_PREDICTION_START_DATE}`."
        )
    return pd.DataFrame(
        {
            "ds": pd.date_range(
                start=DEFAULT_PARKING_PREDICTION_START_DATE,
                end=normalized_end_date,
                freq="D",
            )
        }
    )


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
def newbatch_parking_daily(config: ParkingNewbatchConfig, dwrhs: MITWHRSResource):
    """This asset ingest the new batch of parking activity data from MIT Warehouse"""
    query = _build_newbatch_parking_query(config)
    rows = dwrhs.execute_query(query, chunksize=10000)
    if len(rows) == 0:
        raise Failure("No parking data found!")
    columns = [
        "date",
        "parking_lot",
        "total",
        "unique",
    ]
    df = pd.DataFrame(rows, columns=columns)
    df["last_update"] = datetime.now()
    logger.info(f"Last updated batch parking transaction data since date: {df.date.min()}")
    return Output(
        value=df,
        metadata={
            "last_update": MetadataValue.text(max(df.date).strftime("%Y-%m-%d")),
            "query_start_date": MetadataValue.text(config.start_date),
            "query_end_date": MetadataValue.text(config.end_date or ""),
        },
    )


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def mit_holidays(dwrhs: MITWHRSResource):
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
    rows = dwrhs.execute_query(query, chunksize=100000)
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
def daily_parking_trend(
    config: ParkingTrendConfig,
    df: pd.DataFrame,
    holidays: pd.DataFrame,
):
    """This asset takes in the combined parking data and MIT holidays table
    to forecast the parking trend"""
    df["ds"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None)
    filtered = df[df["ds"].dt.dayofweek < 5].copy()
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
    all_time = _build_parking_prediction_range(config.prediction_end_date)
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
