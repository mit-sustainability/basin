import re
from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import pandera as pa
import sqlalchemy.exc
from dagster import Config, Failure, Output, ResourceParam, asset, get_dagster_logger
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import DateTime, Series

from orchestrator.resources.dropbox import DropboxResource
from orchestrator.resources.postgres_io_manager import PostgreConnResources

logger = get_dagster_logger()

_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")


def _parse_sensor_filename(filename: str) -> dict:
    """Extract location and sensor_id from filename like 'MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx'.

    Pattern: <location tokens> <sensor_id (int)> <YYYY-MM-DD> <time> <tz>.<ext>
    """
    stem = re.sub(r"\.(xlsx|xls|csv)$", "", filename, flags=re.IGNORECASE)
    tokens = stem.split()

    date_idx = next(
        (i for i, t in enumerate(tokens) if _DATE_PATTERN.fullmatch(t)),
        None,
    )
    if date_idx is None or date_idx < 2:
        raise Failure(
            f"Cannot parse sensor metadata from filename: {filename!r}. "
            "Expected format: '<location> <sensor_id> <YYYY-MM-DD> <HH_MM_SS> <tz>'"
        )

    sensor_id = tokens[date_idx - 1]
    location = " ".join(tokens[: date_idx - 1])
    return {"location": location, "sensor_id": sensor_id}


_DIRECT_RENAMES = {
    "#": "row_num",
    "Date-Time (EDT)": "datetime_edt",
    "Date-Time (EST)": "datetime_edt",
    "Date-Time (EDT/EST)": "datetime_edt",
    "Temperature , °C": "temperature_c",
    "Temperature, °C": "temperature_c",
    "temp , °C": "temperature_c",
    "1 , °C": "temperature_c",
    "RH , %": "relative_humidity_pct",
    "RH, %": "relative_humidity_pct",
    "rh , %": "relative_humidity_pct",
    "1 , %": "relative_humidity_pct",
    "Dew Point , °C": "dew_point_c",
    "Dew Point, °C": "dew_point_c",
}

_FAHRENHEIT_RENAMES = {
    "Temperature , °F": "temp_f_raw",
    "Temperature  , °F": "temp_f_raw",
    "Temperature, °F": "temp_f_raw",
    "Dew Point , °F": "dew_point_f_raw",
    "Dew Point  , °F": "dew_point_f_raw",
    "Dew Point, °F": "dew_point_f_raw",
}


def _f_to_c(series: pd.Series) -> pd.Series:
    return (series - 32) * 5 / 9


def _read_sensor_file(file_bytes: BytesIO, meta: dict) -> pd.DataFrame:
    """Load a sensor file (.xlsx/.xls/.csv), normalize all column variants to °C."""
    ext = meta["source_file"].rsplit(".", 1)[-1].lower()
    df = pd.read_csv(file_bytes) if ext == "csv" else pd.read_excel(file_bytes, engine="openpyxl")

    df = df.rename(columns=_DIRECT_RENAMES | _FAHRENHEIT_RENAMES)

    for f_col, c_col in [("temp_f_raw", "temperature_c"), ("dew_point_f_raw", "dew_point_c")]:
        if f_col in df.columns:
            df[c_col] = _f_to_c(df[f_col])
            df = df.drop(columns=f_col)

    if "row_num" not in df.columns:
        df["row_num"] = range(len(df))

    if "dew_point_c" not in df.columns:
        df["dew_point_c"] = float("nan")

    required = {"datetime_edt", "temperature_c", "relative_humidity_pct"}
    missing = required - set(df.columns)
    if missing:
        raise Failure(f"{meta['source_file']}: missing columns {missing} after normalization")

    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])
    df["sensor_id"] = meta["sensor_id"]
    df["location"] = meta["location"]
    df["source_file"] = meta["source_file"]

    return df[[
        "row_num", "datetime_edt", "temperature_c",
        "relative_humidity_pct", "dew_point_c",
        "sensor_id", "location", "source_file",
    ]]


class IndoorHeatSensorRawSchema(pa.DataFrameModel):
    row_num: Series[int] = pa.Field(description="Row number from source file")
    datetime_edt: Series[DateTime] = pa.Field(description="Timestamp of reading (EDT)")
    temperature_c: Series[float] = pa.Field(description="Temperature in Celsius")
    relative_humidity_pct: Series[float] = pa.Field(
        description="Relative Humidity (%)", ge=0, le=100
    )
    dew_point_c: Series[float] = pa.Field(description="Dew Point in Celsius", nullable=True)
    sensor_id: Series[str] = pa.Field(description="Sensor identifier from filename")
    location: Series[str] = pa.Field(description="Location from filename")
    source_file: Series[str] = pa.Field(description="Original filename")
    last_update: Series[DateTime] = pa.Field(description="Ingestion timestamp")


class IndoorHeatConfig(Config):
    dropbox_folder: str = "/MIT Indoor Heat Sensors"


@asset(
    io_manager_key="postgres_append",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(IndoorHeatSensorRawSchema),
)
def raw_indoor_heat_sensor(
    config: IndoorHeatConfig,
    dropbox: DropboxResource,
    pg_engine: ResourceParam[PostgreConnResources],
) -> Output[pd.DataFrame]:
    """Poll Dropbox for new heat sensor files and append to raw table (incremental by filename)."""
    all_files = dropbox.list_sensor_files(config.dropbox_folder)
    if not all_files:
        raise Failure(
            f"No sensor files found in Dropbox folder: {config.dropbox_folder!r}"
        )

    engine = pg_engine.create_engine()
    try:
        processed = set(
            pd.read_sql_query(
                "SELECT DISTINCT source_file FROM raw.indoor_heat_sensor", engine
            )["source_file"].tolist()
        )
    except sqlalchemy.exc.ProgrammingError:
        processed = set()

    new_files = [(name, path) for name, path in all_files if name not in processed]
    logger.info(f"{len(new_files)} new files to process (skipping {len(processed)} already ingested)")

    if not new_files:
        empty_df = pd.DataFrame({
            "row_num": pd.array([], dtype="int64"),
            "datetime_edt": pd.array([], dtype="datetime64[ns]"),
            "temperature_c": pd.array([], dtype="float64"),
            "relative_humidity_pct": pd.array([], dtype="float64"),
            "dew_point_c": pd.array([], dtype="float64"),
            "sensor_id": pd.array([], dtype="object"),
            "location": pd.array([], dtype="object"),
            "source_file": pd.array([], dtype="object"),
            "last_update": pd.array([], dtype="datetime64[ns]"),
        })
        return Output(
            value=empty_df,
            metadata={"new_files": 0, "total_files_in_dropbox": len(all_files)},
        )

    frames = []
    for name, path in new_files:
        try:
            meta = _parse_sensor_filename(name)
            meta["source_file"] = name
            file_bytes = dropbox.download_file(path)
            df = _read_sensor_file(file_bytes, meta)
            frames.append(df)
            logger.info(f"Processed {name}: {len(df)} rows")
        except Exception as exc:
            logger.warning(f"Skipping {name}: {exc}")

    if not frames:
        raise Failure("All new files failed to parse — check logs for details")

    combined = pd.concat(frames, ignore_index=True)
    combined["last_update"] = datetime.now()

    return Output(
        value=combined,
        metadata={
            "new_files": len(new_files),
            "total_rows": len(combined),
            "sensors": combined["sensor_id"].nunique(),
            "date_range_start": str(combined["datetime_edt"].min()),
            "date_range_end": str(combined["datetime_edt"].max()),
        },
    )


@asset(
    deps=[raw_indoor_heat_sensor],
    io_manager_key="postgres_replace",
    compute_kind="python",
    key_prefix="staging",
    group_name="staging",
)
def stg_indoor_heat(pg_engine: ResourceParam[PostgreConnResources]) -> Output[pd.DataFrame]:
    """Deduplicate raw heat sensor readings on (sensor_id, datetime_edt)."""
    engine = pg_engine.create_engine()
    df = pd.read_sql_query("SELECT * FROM raw.indoor_heat_sensor", engine)

    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])
    df["temperature_c"] = df["temperature_c"].astype(float)
    df["relative_humidity_pct"] = df["relative_humidity_pct"].astype(float)
    df["dew_point_c"] = df["dew_point_c"].astype(float)

    before = len(df)
    df = df.sort_values("last_update", ascending=False).drop_duplicates(
        subset=["sensor_id", "datetime_edt"], keep="first"
    )
    logger.info(f"Deduplicated {before} -> {len(df)} rows")

    out_cols = [
        "sensor_id", "location", "datetime_edt",
        "temperature_c", "relative_humidity_pct", "dew_point_c",
        "source_file", "last_update",
    ]
    return Output(
        value=df[out_cols],
        metadata={"total_rows": len(df), "unique_sensors": df["sensor_id"].nunique()},
    )
