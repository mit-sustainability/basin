import json
import re
from datetime import datetime
from io import BytesIO

import numpy as np
import pandas as pd
import pandera as pa
from dagster import Config, Failure, Output, ResourceParam, asset, get_dagster_logger
from dagster_pandera import pandera_schema_to_dagster_type
from pandera.typing import DateTime, Series

from orchestrator.resources.dropbox import DropboxResource
from orchestrator.resources.postgres_io_manager import PostgreConnResources

logger = get_dagster_logger()

_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")


def _parse_sensor_filename(filename: str) -> dict:
    """Extract sensor_id from a HOBO export filename.

    Handles two formats:
      - '<location> <sensor_id> <YYYY-MM-DD> <HH_MM_SS> <tz>.<ext>'  (e.g. 'MIT+Camb 3 2026-05-15 ...')
      - '<sensor_id> <YYYY-MM-DD> <HH_MM_SS> <tz>.<ext>'             (e.g. '22086523 2025-11-24 ...')
    """
    stem = re.sub(r"\.(xlsx|xls|csv)$", "", filename, flags=re.IGNORECASE)
    tokens = stem.split()

    date_idx = next(
        (i for i, t in enumerate(tokens) if _DATE_PATTERN.fullmatch(t)),
        None,
    )
    if date_idx is None or date_idx < 1:
        raise Failure(
            f"Cannot parse sensor metadata from filename: {filename!r}. "
            "Expected: '<sensor_id> <YYYY-MM-DD> ...' or '<location> <sensor_id> <YYYY-MM-DD> ...'"
        )

    return {"sensor_id": tokens[date_idx - 1]}


_DIRECT_RENAMES = {
    "#": "row_num",
    "Date-Time (EDT)": "datetime_edt",
    "Date-Time (EST)": "datetime_edt",
    "Date-Time (EDT/EST)": "datetime_edt",
    "Date-Time (EST/EDT)": "datetime_edt",
    "Temperature , °C": "temperature_c",
    "Temperature, °C": "temperature_c",
    "Temperature   (°C)": "temperature_c",
    "temp , °C": "temperature_c",
    "1 , °C": "temperature_c",
    "RH , %": "relative_humidity_pct",
    "RH, %": "relative_humidity_pct",
    "RH   (%)": "relative_humidity_pct",
    "rh , %": "relative_humidity_pct",
    "1 , %": "relative_humidity_pct",
    "Dew Point , °C": "dew_point_c",
    "Dew Point, °C": "dew_point_c",
    "Dew Point   (°C)": "dew_point_c",
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

    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])
    df["sensor_id"] = meta["sensor_id"]
    df["source_file"] = meta["source_file"]

    return df[[
        "row_num", "datetime_edt", "temperature_c",
        "relative_humidity_pct", "dew_point_c",
        "sensor_id", "source_file",
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
    source_file: Series[str] = pa.Field(description="Original filename")
    last_update: Series[DateTime] = pa.Field(description="Ingestion timestamp")


class IndoorHeatConfig(Config):
    dropbox_folder: str = "ns:4039652928/Program Topics/Data/Projects/Outdoor campus heat data 2025/HOBO_Data END OF SEASON COLLECTION"


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=pandera_schema_to_dagster_type(IndoorHeatSensorRawSchema),
)
def indoor_heat_sensor(
    config: IndoorHeatConfig,
    dropbox: DropboxResource,
) -> Output[pd.DataFrame]:
    """Load all sensor files from Dropbox and replace the raw table."""
    all_files = dropbox.list_sensor_files(config.dropbox_folder)
    if not all_files:
        raise Failure(
            f"No sensor files found in Dropbox folder: {config.dropbox_folder!r}"
        )

    frames = []
    for name, path in all_files:
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


class SensorConfigPath(Config):
    config_file_path: str = "ns:4039652928/Program Topics/Data/Projects/Outdoor campus heat data 2025/config_hobo.json"


def _load_sensor_metadata(dropbox: DropboxResource, config_file_path: str) -> pd.DataFrame:
    raw = json.loads(dropbox.download_file(config_file_path).read().decode())
    rows = []
    for sid, meta in raw.items():
        rows.append({
            "sensor_id": sid,
            "sensor_name": meta.get("name"),
            "lat": meta.get("coords", [None, None])[0],
            "lon": meta.get("coords", [None, None])[1],
            "deployment": meta.get("deployment"),
            "radiation_shield": meta.get("radiation_shield") or meta.get("rediation_shield"),
        })
    return pd.DataFrame(rows)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def indoor_heat_sensor_config(
    config: SensorConfigPath,
    dropbox: DropboxResource,
) -> Output[pd.DataFrame]:
    """Load sensor site metadata from config_hobo.json into a raw lookup table."""
    df = _load_sensor_metadata(dropbox, config.config_file_path)
    return Output(
        value=df,
        metadata={"sensors": len(df)},
    )


@asset(
    deps=[indoor_heat_sensor],
    io_manager_key="postgres_replace",
    compute_kind="python",
    key_prefix="staging",
    group_name="staging",
)
def stg_indoor_heat_aligned(pg_engine: ResourceParam[PostgreConnResources]) -> Output[pd.DataFrame]:
    """Deduplicate, normalize to °F with heat index, bin to 20-min intervals."""
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


_CALIB_VARS = ["temperature_f", "relative_humidity_pct", "heat_index_f"]
_OUTLIER_SIGMA = 2.0
_EXCLUDED_SIGMA = 3.0


def _compute_calibration_stats(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Cross-sensor precision and outlier analysis.

    Returns (precision_df, sensor_stats_df).
    """
    # Per-timestamp ensemble mean across all sensors
    grouped = df.groupby("datetime_edt")[_CALIB_VARS]
    ts_mean = grouped.mean().rename(columns={v: f"{v}_mean" for v in _CALIB_VARS})
    ts_std = grouped.std().rename(columns={v: f"{v}_std" for v in _CALIB_VARS})
    ts_stats = ts_mean.join(ts_std).reset_index()

    # Precision summary: average cross-sensor σ across all timestamps
    precision_rows = []
    for var in _CALIB_VARS:
        std_col = ts_stats[f"{var}_std"].dropna()
        precision_rows.append({
            "variable": var,
            "mean_sigma": round(float(std_col.mean()), 4),
            "median_sigma": round(float(std_col.median()), 4),
            "max_sigma": round(float(std_col.max()), 4),
        })
    precision_df = pd.DataFrame(precision_rows)

    # Per-sensor bias vs ensemble mean
    ensemble = ts_stats[["datetime_edt"] + [f"{v}_mean" for v in _CALIB_VARS]]
    merged = df.merge(ensemble, on="datetime_edt", how="left")

    sensor_rows = []
    for sid, grp in merged.groupby("sensor_id"):
        row: dict = {"sensor_id": sid}
        for var in _CALIB_VARS:
            residuals = grp[var] - grp[f"{var}_mean"]
            row[f"{var}_bias"] = round(float(residuals.mean()), 4)
            row[f"{var}_std"] = round(float(residuals.std()), 4)
        sensor_rows.append(row)

    sensor_stats = pd.DataFrame(sensor_rows)
    sensor_stats["is_outlier"] = False
    sensor_stats["severity"] = "pass"

    # Flag outliers: |bias| > _OUTLIER_SIGMA × std-of-biases across sensors
    for var in _CALIB_VARS:
        col = sensor_stats[f"{var}_bias"]
        sigma = col.std()
        n_sigma = col.abs() / sigma
        sensor_stats[f"{var}_n_sigma"] = round(n_sigma, 2)
        sensor_stats[f"{var}_is_outlier"] = n_sigma > _OUTLIER_SIGMA
        sensor_stats["is_outlier"] |= sensor_stats[f"{var}_is_outlier"]

    # Severity: worst n_sigma across all variables
    for idx, row in sensor_stats.iterrows():
        max_n = max(row[f"{v}_n_sigma"] for v in _CALIB_VARS)
        if max_n > _EXCLUDED_SIGMA:
            sensor_stats.at[idx, "severity"] = "excluded"
        elif max_n > _OUTLIER_SIGMA:
            sensor_stats.at[idx, "severity"] = "marginal"

    return precision_df, sensor_stats


@asset(
    deps=[stg_indoor_heat_aligned],
    compute_kind="python",
    group_name="calibration",
)
def indoor_heat_calibration(pg_engine: ResourceParam[PostgreConnResources]) -> None:
    """Cross-sensor calibration report. Writes two tables to Postgres. Run manually only."""
    engine = pg_engine.create_engine()
    df = pd.read_sql_query("SELECT * FROM staging.stg_indoor_heat_aligned", engine)
    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])

    precision_df, sensor_stats = _compute_calibration_stats(df)

    run_at = pd.Timestamp.now()
    precision_df["run_at"] = run_at
    sensor_stats["run_at"] = run_at

    precision_df.to_sql(
        "indoor_heat_calibration_precision", engine,
        schema="staging", if_exists="replace", index=False,
    )
    sensor_stats.to_sql(
        "indoor_heat_calibration_sensors", engine,
        schema="staging", if_exists="replace", index=False,
    )

    outlier_count = int(sensor_stats["is_outlier"].sum())
    logger.info(
        f"Calibration complete: {len(sensor_stats)} sensors, {outlier_count} outliers"
    )
