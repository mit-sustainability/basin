"""
Standalone HOBO sensor data processor.

Usage:
    python process_heat_data.py <folder> [--output out.csv] [--bin-minutes 20]

Loads all .csv / .xlsx / .xls files in <folder>, normalizes column names,
deduplicates, bins to fixed-width intervals, computes heat index, and writes
a single merged CSV.

Sensor ID is parsed from the filename:
  - '<location> <sensor_id> YYYY-MM-DD ...'  → sensor_id = token before date
  - '<sensor_id> YYYY-MM-DD ...'             → sensor_id = token before date
  Falls back to the full filename stem if no date token is found.
"""

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")

_DIRECT_RENAMES = {
    "#": "row_num",
    "Date-Time (EDT)": "datetime_edt",
    "Date-Time (EST)": "datetime_edt",
    "Date-Time (EDT/EST)": "datetime_edt",
    "Date-Time (EST/EDT)": "datetime_edt",
    "Temperature , °C": "temperature_c",
    "Temperature, °C": "temperature_c",
    "Temperature (°C)": "temperature_c",
    "Temperature   (°C)": "temperature_c",
    "temp , °C": "temperature_c",
    "temp (°C)": "temperature_c",
    "1 , °C": "temperature_c",
    "1 (°C)": "temperature_c",
    "RH , %": "relative_humidity_pct",
    "RH, %": "relative_humidity_pct",
    "RH (%)": "relative_humidity_pct",
    "RH   (%)": "relative_humidity_pct",
    "rh , %": "relative_humidity_pct",
    "rh (%)": "relative_humidity_pct",
    "1 , %": "relative_humidity_pct",
    "1 (%)": "relative_humidity_pct",
    "Dew Point , °C": "dew_point_c",
    "Dew Point, °C": "dew_point_c",
    "Dew Point (°C)": "dew_point_c",
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


def _sensor_id_from_filename(filename: str) -> str:
    stem = re.sub(r"\.(xlsx|xls|csv)$", "", filename, flags=re.IGNORECASE)
    tokens = stem.split()
    date_idx = next((i for i, t in enumerate(tokens) if _DATE_PATTERN.fullmatch(t)), None)
    if date_idx is not None and date_idx >= 1:
        return tokens[date_idx - 1]
    # ponytail: fall back to full stem — beats crashing on unexpected filenames
    return stem


def _f_to_c(s: pd.Series) -> pd.Series:
    return (s - 32) * 5 / 9


def _heat_index_f(temp_f: pd.Series, rh: pd.Series) -> pd.Series:
    """NOAA/Rothfusz regression. Inputs and output in °F."""
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


def _read_file(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext == ".xls":
        df = pd.read_excel(path, engine="xlrd")
    else:
        df = pd.read_excel(path, engine="openpyxl")

    df.columns = [re.sub(r"\s+", " ", c).strip() for c in df.columns]
    df = df.rename(columns=_DIRECT_RENAMES | _FAHRENHEIT_RENAMES)
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    for f_col, c_col in [("temp_f_raw", "temperature_c"), ("dew_point_f_raw", "dew_point_c")]:
        if f_col in df.columns:
            if c_col not in df.columns:
                df[c_col] = _f_to_c(df[f_col])
            df = df.drop(columns=f_col)

    if "dew_point_c" not in df.columns:
        df["dew_point_c"] = float("nan")

    required = {"datetime_edt", "temperature_c", "relative_humidity_pct"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{path.name}: missing columns after normalization: {missing}")

    df = df.dropna(subset=["temperature_c", "relative_humidity_pct"])
    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])
    df["sensor_id"] = _sensor_id_from_filename(path.name)
    df["source_file"] = path.name

    return df[["datetime_edt", "temperature_c", "relative_humidity_pct", "dew_point_c", "sensor_id", "source_file"]]


def process(folder: str, output: str, bin_minutes: int) -> None:
    folder_path = Path(folder)
    files = sorted(p for p in folder_path.iterdir() if p.suffix.lower() in {".csv", ".xlsx", ".xls"})
    if not files:
        sys.exit(f"No sensor files found in {folder!r}")

    frames = []
    for f in files:
        try:
            frames.append(_read_file(f))
            print(f"  loaded {f.name}: {len(frames[-1])} rows")
        except Exception as exc:
            print(f"  WARNING skipping {f.name}: {exc}", file=sys.stderr)

    if not frames:
        sys.exit("All files failed to parse — check warnings above")

    df = pd.concat(frames, ignore_index=True)

    # Dedup: last-seen row wins for (sensor_id, datetime) duplicates
    df = df.drop_duplicates(subset=["sensor_id", "datetime_edt"], keep="last")

    df["temperature_f"] = df["temperature_c"] * 9 / 5 + 32
    df["dew_point_f"] = df["dew_point_c"] * 9 / 5 + 32
    df["heat_index_f"] = _heat_index_f(df["temperature_f"], df["relative_humidity_pct"])

    freq = f"{bin_minutes}min"
    df["datetime_bin"] = df["datetime_edt"].dt.round(freq)
    aligned = (
        df.groupby(["sensor_id", "datetime_bin"])
        .agg(
            temperature_c=("temperature_c", "mean"),
            temperature_f=("temperature_f", "mean"),
            relative_humidity_pct=("relative_humidity_pct", "mean"),
            dew_point_c=("dew_point_c", "mean"),
            dew_point_f=("dew_point_f", "mean"),
            heat_index_f=("heat_index_f", "mean"),
        )
        .reset_index()
        .rename(columns={"datetime_bin": "datetime_edt"})
    )

    aligned.to_csv(output, index=False)
    print(f"\n{len(aligned)} rows → {output}  ({aligned['sensor_id'].nunique()} sensors, "
          f"{aligned['datetime_edt'].min()} – {aligned['datetime_edt'].max()})")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge and clean HOBO heat sensor files")
    parser.add_argument("folder", help="Directory containing .csv/.xlsx/.xls sensor exports")
    parser.add_argument("--output", default="heat_combined.csv", help="Output CSV path (default: heat_combined.csv)")
    parser.add_argument("--bin-minutes", type=int, default=20, help="Timestamp bin size in minutes (default: 20)")
    args = parser.parse_args()
    process(args.folder, args.output, args.bin_minutes)
