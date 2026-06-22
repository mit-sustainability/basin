import json
import os
import re
from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import pandera as pa
from dagster import AssetKey, Config, Failure, Output, ResourceParam, asset, get_dagster_logger
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import DateTime, Series

from orchestrator.resources.arcgis import ArcGISResource
from orchestrator.resources.dropbox import DropboxResource
from orchestrator.resources.postgres_io_manager import PostgreConnResources

logger = get_dagster_logger()

_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")

_AGOL_LAYER_URL = os.getenv("ARCGIS_OUTDOOR_HEAT_LAYER_URL", "")
_AGOL_TIMESERIES_URL = os.getenv("ARCGIS_OUTDOOR_HEAT_TIMESERIES_URL", "")

_TIMESERIES_QUERY = """
    SELECT sensor_id, datetime_edt, temperature_f, relative_humidity_pct,
           dew_point_f, heat_index_f, sensor_name, deployment
    FROM final.final_outdoor_heat_combined
    ORDER BY sensor_id, datetime_edt
"""

_OUTDOOR_EXPORT_QUERY = """
    SELECT DISTINCT ON (r.sensor_id)
        r.sensor_id, r.datetime_edt, r.temperature_f, r.relative_humidity_pct,
        r.dew_point_f, r.heat_index_f,
        c.sensor_name, c.lat, c.lon, c.deployment, c.radiation_shield
    FROM staging.stg_outdoor_heat_aligned r
    JOIN raw.outdoor_heat_sensor_config c ON r.sensor_id = c.sensor_id
    ORDER BY r.sensor_id, r.datetime_edt DESC
"""


def _extract_filename_prefix(filename: str) -> str:
    """Extract location name — everything before the date in the filename, stripped."""
    stem = re.sub(r"\.(xlsx|xls|csv)$", "", filename, flags=re.IGNORECASE)
    match = _DATE_PATTERN.search(stem)
    if not match:
        raise Failure(
            f"Cannot find date in filename: {filename!r}. "
            "Expected format: '<location> YYYY-MM-DD ...'"
        )
    return stem[:match.start()].strip()


_DIRECT_RENAMES = {
    "#": "row_num",
    "Date-Time (EDT)": "datetime_edt",
    "Date-Time (EST)": "datetime_edt",
    "Date-Time (EDT/EST)": "datetime_edt",
    "Date-Time (EST/EDT)": "datetime_edt",
    "Temperature , °C": "temperature_c",
    "Temperature, °C": "temperature_c",
    "Temperature (°C)": "temperature_c",
    "temp , °C": "temperature_c",
    "temp (°C)": "temperature_c",
    "1 , °C": "temperature_c",
    "1 (°C)": "temperature_c",
    "RH , %": "relative_humidity_pct",
    "RH, %": "relative_humidity_pct",
    "RH (%)": "relative_humidity_pct",
    "rh , %": "relative_humidity_pct",
    "rh (%)": "relative_humidity_pct",
    "1 , %": "relative_humidity_pct",
    "1 (%)": "relative_humidity_pct",
    "Dew Point , °C": "dew_point_c",
    "Dew Point, °C": "dew_point_c",
    "Dew Point (°C)": "dew_point_c",
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


def _calculate_heat_index_f(temp_f: pd.Series, rh: pd.Series) -> pd.Series:
    """NOAA/Rothfusz heat index. Inputs and output in °F."""
    hi_simple = 0.5 * (temp_f + 61.0 + ((temp_f - 68.0) * 1.2) + (rh * 0.094))
    hi_simple = (hi_simple + temp_f) / 2

    c1, c2, c3, c4 = -42.379, 2.04901523, 10.14333127, -0.22475541
    c5, c6, c7, c8, c9 = -0.00683783, -0.05481717, 0.00122874, 0.00085282, -0.00000199
    hi_full = (c1 + c2 * temp_f + c3 * rh + c4 * temp_f * rh
               + c5 * temp_f**2 + c6 * rh**2
               + c7 * temp_f**2 * rh + c8 * temp_f * rh**2
               + c9 * temp_f**2 * rh**2)

    adj_low = ((13 - rh) / 4) * np.sqrt(np.maximum(0, (17 - np.abs(temp_f - 95)) / 17))
    hi_full = np.where((rh < 13) & (temp_f >= 80) & (temp_f <= 112), hi_full - adj_low, hi_full)

    adj_high = ((rh - 85) / 10) * ((87 - temp_f) / 5)
    hi_full = np.where((rh > 85) & (temp_f >= 80) & (temp_f <= 87), hi_full + adj_high, hi_full)

    return pd.Series(np.where(hi_simple < 80, hi_simple, hi_full), index=temp_f.index)


def _read_sensor_file(file_bytes: BytesIO, meta: dict) -> pd.DataFrame:
    """Load a sensor file (.xlsx/.xls/.csv), normalize all column variants to °C."""
    ext = meta["source_file"].rsplit(".", 1)[-1].lower()
    if ext == "csv":
        df = pd.read_csv(file_bytes)
    elif ext == "xls":
        df = pd.read_excel(file_bytes, engine="xlrd")
    else:
        df = pd.read_excel(file_bytes, engine="openpyxl")

    df.columns = [re.sub(r"\s+", " ", c).strip() for c in df.columns]
    df = df.rename(columns=_DIRECT_RENAMES | _FAHRENHEIT_RENAMES)
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    for f_col, c_col in [("temp_f_raw", "temperature_c"), ("dew_point_f_raw", "dew_point_c")]:
        if f_col in df.columns:
            if c_col not in df.columns:
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

    df = df.dropna(subset=["temperature_c", "relative_humidity_pct"])

    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])
    df["sensor_id"] = meta["sensor_id"]
    df["source_file"] = meta["source_file"]

    return df[[
        "row_num", "datetime_edt", "temperature_c",
        "relative_humidity_pct", "dew_point_c",
        "sensor_id", "source_file",
    ]]


class OutdoorHeatSensorRawSchema(pa.DataFrameModel):
    row_num: Series[int] = pa.Field(description="Row number from source file")
    datetime_edt: Series[DateTime] = pa.Field(description="Timestamp of reading (EDT)")
    temperature_c: Series[float] = pa.Field(description="Temperature in Celsius")
    relative_humidity_pct: Series[float] = pa.Field(
        description="Relative Humidity (%)", ge=0, le=100
    )
    dew_point_c: Series[float] = pa.Field(description="Dew Point in Celsius", nullable=True)
    sensor_id: Series[str] = pa.Field(description="Sensor identifier from filename")
    source_file: Series[str] = pa.Field(description="Original filename")
    last_update: Series[DateTime] = pa.Field(description="Ingestion timestamp")


class OutdoorHeatConfig(Config):
    dropbox_folder: str = "ns:4039652928/Program Topics/Data/Projects/Outdoor campus heat data 2026/Latest"
    config_file_path: str = "ns:4039652928/Program Topics/Data/Projects/Outdoor campus heat data 2026/outdoor_sensor_config.json"


class OutdoorSensorConfigPath(Config):
    config_file_path: str = "ns:4039652928/Program Topics/Data/Projects/Outdoor campus heat data 2026/outdoor_sensor_config.json"


def _load_sensor_metadata(dropbox: DropboxResource, config_file_path: str) -> pd.DataFrame:
    raw = json.loads(dropbox.download_file(config_file_path).read().decode())
    rows = []
    for sid, meta in raw.items():
        rows.append({
            "sensor_id": sid,
            "filename_match": meta["filename_match"],
            "sensor_name": meta.get("name"),
            "lat": meta.get("lat"),
            "lon": meta.get("lon"),
            "deployment": meta.get("zone"),
            "radiation_shield": meta["radiation_shield"] if "radiation_shield" in meta else None,
        })
    df = pd.DataFrame(rows)
    df["radiation_shield"] = pd.array(df["radiation_shield"].tolist(), dtype=pd.BooleanDtype())
    return df


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def outdoor_heat_sensor_config(
    config: OutdoorSensorConfigPath,
    dropbox: DropboxResource,
) -> Output[pd.DataFrame]:
    """Load outdoor sensor site metadata from config JSON into a raw lookup table."""
    df = _load_sensor_metadata(dropbox, config.config_file_path)
    return Output(
        value=df,
        metadata={"sensors": len(df)},
    )


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(OutdoorHeatSensorRawSchema),
)
def outdoor_heat_sensor(
    config: OutdoorHeatConfig,
    dropbox: DropboxResource,
) -> Output[pd.DataFrame]:
    """Load all outdoor sensor files from Dropbox and replace the raw table."""
    sensor_cfg = _load_sensor_metadata(dropbox, config.config_file_path)
    filename_to_sensor_id = dict(zip(sensor_cfg["filename_match"], sensor_cfg["sensor_id"]))

    all_files = dropbox.list_sensor_files(config.dropbox_folder)
    if not all_files:
        raise Failure(
            f"No sensor files found in Dropbox folder: {config.dropbox_folder!r}"
        )

    frames = []
    for name, path in all_files:
        try:
            prefix = _extract_filename_prefix(name)
            sensor_id = filename_to_sensor_id.get(prefix)
            if sensor_id is None:
                raise Failure(f"No config entry for filename prefix: {prefix!r}")
            file_bytes = dropbox.download_file(path)
            df = _read_sensor_file(file_bytes, {"sensor_id": sensor_id, "source_file": name})
            frames.append(df)
            logger.info(f"Processed {name}: {len(df)} rows")
        except Exception as exc:
            logger.warning(f"Skipping {name}: {exc}")

    if not frames:
        raise Failure("All files failed to parse — check logs for details")

    combined = pd.concat(frames, ignore_index=True)
    combined["last_update"] = datetime.now()

    return Output(
        value=combined,
        metadata={
            "total_files": len(all_files),
            "total_rows": len(combined),
            "sensors": combined["sensor_id"].nunique(),
            "date_range_start": str(combined["datetime_edt"].min()),
            "date_range_end": str(combined["datetime_edt"].max()),
        },
    )


@asset(
    deps=[outdoor_heat_sensor],
    io_manager_key="postgres_replace",
    compute_kind="python",
    key_prefix="staging",
    group_name="staging",
)
def stg_outdoor_heat_aligned(pg_engine: ResourceParam[PostgreConnResources]) -> Output[pd.DataFrame]:
    """Deduplicate, normalize to °F with heat index, bin to 20-min intervals."""
    engine = pg_engine.create_engine()
    df = pd.read_sql_query("SELECT * FROM raw.outdoor_heat_sensor", engine)

    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])
    df["temperature_c"] = df["temperature_c"].astype(float)
    df["relative_humidity_pct"] = df["relative_humidity_pct"].astype(float)
    df["dew_point_c"] = df["dew_point_c"].astype(float)

    before = len(df)
    df = df.sort_values("last_update", ascending=False).drop_duplicates(
        subset=["sensor_id", "datetime_edt"], keep="first"
    )
    logger.info(f"Deduplicated {before} -> {len(df)} rows")

    df["temperature_f"] = df["temperature_c"] * 9 / 5 + 32
    df["dew_point_f"] = df["dew_point_c"] * 9 / 5 + 32
    df["heat_index_f"] = _calculate_heat_index_f(df["temperature_f"], df["relative_humidity_pct"])

    df["datetime_bin"] = df["datetime_edt"].dt.round("20min")
    aligned = (
        df.groupby(["sensor_id", "datetime_bin"])
        .agg(
            temperature_f=("temperature_f", "mean"),
            relative_humidity_pct=("relative_humidity_pct", "mean"),
            dew_point_f=("dew_point_f", "mean"),
            heat_index_f=("heat_index_f", "mean"),
        )
        .reset_index()
        .rename(columns={"datetime_bin": "datetime_edt"})
    )

    return Output(
        value=aligned,
        metadata={
            "total_rows": len(aligned),
            "unique_sensors": aligned["sensor_id"].nunique(),
            "date_range_start": str(aligned["datetime_edt"].min()),
            "date_range_end": str(aligned["datetime_edt"].max()),
        },
    )


@asset(
    deps=[AssetKey(["final", "final_outdoor_heat_combined"]), "outdoor_heat_sensor_config"],
    compute_kind="python",
    group_name="exports",
)
def agol_outdoor_heat_sync(
    pg_engine: ResourceParam[PostgreConnResources],
    arcgis: ResourceParam[ArcGISResource],
) -> Output[None]:
    """Full-replace the ArcGIS Online outdoor heat feature layer with current data."""
    if not _AGOL_LAYER_URL:
        raise Failure("ARCGIS_OUTDOOR_HEAT_LAYER_URL env var is not set")

    engine = pg_engine.create_engine()
    with engine.connect() as conn:
        df = pd.read_sql(_OUTDOOR_EXPORT_QUERY, conn)

    df["datetime_edt"] = df["datetime_edt"].astype(str)

    result = arcgis.replace_features(
        layer_url=_AGOL_LAYER_URL,
        df=df,
        geometry_fields=("lat", "lon"),
    )

    return Output(
        value=None,
        metadata={
            "features_deleted": result["deleted"],
            "features_added": result["added"],
            "total_rows_synced": len(df),
        },
    )


@asset(
    deps=[AssetKey(["final", "final_outdoor_heat_combined"]), "outdoor_heat_sensor_config"],
    compute_kind="python",
    group_name="exports",
)
def agol_outdoor_heat_timeseries(
    pg_engine: ResourceParam[PostgreConnResources],
    arcgis: ResourceParam[ArcGISResource],
) -> Output[None]:
    """Full-replace the ArcGIS Online outdoor heat time series table with all readings."""
    if not _AGOL_TIMESERIES_URL:
        raise Failure("ARCGIS_OUTDOOR_HEAT_TIMESERIES_URL env var is not set")

    engine = pg_engine.create_engine()
    with engine.connect() as conn:
        df = pd.read_sql(_TIMESERIES_QUERY, conn)

    df["datetime_edt"] = df["datetime_edt"].astype(str)

    result = arcgis.replace_features(
        layer_url=_AGOL_TIMESERIES_URL,
        df=df,
        geometry_fields=None,
    )

    return Output(
        value=None,
        metadata={
            "features_deleted": result["deleted"],
            "features_added": result["added"],
            "total_rows_synced": len(df),
            "unique_sensors": df["sensor_id"].nunique(),
        },
    )
