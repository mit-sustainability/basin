"""Export one indoor-heat phase to JSON + manifest.

Usage:
    python scripts/export_indoor_phase.py --phase phase1

Fill in PHASES below with the Dropbox paths the student provides for each phase.
"""

import argparse
import json
import logging
import os
import re
import tempfile
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import dropbox
import numpy as np
import pandas as pd
from dropbox.common import PathRoot

logger = logging.getLogger(__name__)

# ── Vendored from orchestrator (avoids triggering Dagster/DBT imports) ────────

_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")

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
    "temp   (°C)": "temperature_c",
    "1 , °C": "temperature_c",
    "RH , %": "relative_humidity_pct",
    "RH, %": "relative_humidity_pct",
    "RH   (%)": "relative_humidity_pct",
    "rh , %": "relative_humidity_pct",
    "rh   (%)": "relative_humidity_pct",
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


def _parse_sensor_filename(filename: str) -> dict:
    """Extract sensor_id from a HOBO export filename."""
    stem = re.sub(r"\.(xlsx|xls|csv)$", "", filename, flags=re.IGNORECASE)
    tokens = stem.split()
    date_idx = next((i for i, t in enumerate(tokens) if _DATE_PATTERN.fullmatch(t)), None)
    if date_idx is None or date_idx < 1:
        raise ValueError(f"Cannot parse sensor metadata from filename: {filename!r}")
    return {"sensor_id": tokens[date_idx - 1]}


def _read_sensor_file(file_bytes: BytesIO, meta: dict) -> pd.DataFrame:
    """Load a HOBO sensor file (.xlsx/.xls/.csv), normalize columns to °C."""
    header = file_bytes.read(4)
    file_bytes.seek(0)
    is_xlsx = header[:2] == b"PK"
    is_xls = header[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"[:8]
    ext = meta["source_file"].rsplit(".", 1)[-1].lower()
    if is_xlsx:
        df = pd.read_excel(file_bytes, engine="openpyxl")
    elif is_xls or ext == "xls":
        df = pd.read_excel(file_bytes, engine="xlrd")
    else:
        df = pd.read_csv(file_bytes)

    df = df.rename(columns=_DIRECT_RENAMES | _FAHRENHEIT_RENAMES)
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    reading_cols = [c for c in ["temperature_c", "temp_f_raw", "relative_humidity_pct"] if c in df.columns]
    if reading_cols:
        df = df[df[reading_cols].notna().any(axis=1)]

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
        raise ValueError(f"{meta['source_file']}: missing columns {missing} after normalization")

    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])
    df["sensor_id"] = meta["sensor_id"]
    df["source_file"] = meta["source_file"]

    return df[["row_num", "datetime_edt", "temperature_c", "relative_humidity_pct", "dew_point_c", "sensor_id", "source_file"]]


def _load_sensor_metadata(dropbox_res: "DropboxResource", config_file_path: str) -> pd.DataFrame:
    """Load JSON sensor config keyed by room/sensor ID."""
    raw = json.loads(dropbox_res.download_file(config_file_path).read().decode())
    rows = []
    for sid, meta in raw.items():
        rows.append({
            "sensor_id": sid,
            "floor": meta.get("floor"),
            "orientation": meta.get("orientation"),
            "window_state": meta.get("window_state"),
            "blinds_state": meta.get("blinds_state"),
            "note": meta.get("note"),
            "sensor_photo": meta.get("sensor_photo"),
            "window_photo": meta.get("window_photo"),
            "node_x": meta.get("node_x"),
            "node_y": meta.get("node_y"),
            "skip_start": pd.to_datetime(meta.get("skip_start"), errors="coerce"),
            "skip_end": pd.to_datetime(meta.get("skip_end"), errors="coerce"),
        })
    return pd.DataFrame(rows)


_NS_RE = re.compile(r"^ns:(\d+)(/.*)? *$")


class DropboxResource:
    def _client(self):
        return dropbox.Dropbox(
            oauth2_refresh_token=os.environ["DROPBOX_REFRESH_TOKEN"],
            app_key=os.environ["DROPBOX_APP_KEY"],
            app_secret=os.environ["DROPBOX_APP_SECRET"],
        )

    def _resolve(self, folder_path: str):
        m = _NS_RE.match(folder_path.strip())
        if m:
            ns_id = m.group(1)
            subpath = m.group(2) or "/"
            return self._client().with_path_root(PathRoot.namespace_id(ns_id)), subpath
        return self._client(), folder_path

    def _list_files(self, folder_path: str, extensions: tuple) -> list[tuple[str, str]]:
        dbx, path = self._resolve(folder_path)
        m = _NS_RE.match(folder_path.strip())
        ns_prefix = f"ns:{m.group(1)}" if m else ""
        result = dbx.files_list_folder(path)
        entries = list(result.entries)
        while result.has_more:
            result = dbx.files_list_folder_continue(result.cursor)
            entries.extend(result.entries)
        return [(e.name, f"{ns_prefix}{e.path_lower}") for e in entries
                if hasattr(e, "name") and e.name.lower().endswith(extensions)]

    def list_sensor_files(self, folder_path: str) -> list[tuple[str, str]]:
        return self._list_files(folder_path, (".xlsx", ".xls", ".csv"))

    def download_file(self, path: str) -> BytesIO:
        dbx, resolved_path = self._resolve(path)
        _, response = dbx.files_download(resolved_path)
        try:
            return BytesIO(response.content)
        finally:
            response.close()

# ─────────────────────────────────────────────────────────────────────────────

_EDT = timezone(timedelta(hours=-4))


def _parse_edt(raw) -> str:
    if isinstance(raw, str):
        dt = datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=_EDT)
    else:
        dt = pd.Timestamp(raw).to_pydatetime().replace(tzinfo=_EDT)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_phase_export(output_dir: Path, df: pd.DataFrame, now: datetime, browser_base: str = "/data") -> tuple[Path, dict]:
    """Write readings JSON + manifest, including device_type and wbgt_f columns."""
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    readings_filename = f"readings_{stamp}.json"
    readings_path = output_dir / readings_filename
    browser_path = f"{browser_base}/{readings_filename}"
    output_dir.mkdir(parents=True, exist_ok=True)

    records = []
    for _, row in df.iterrows():
        if pd.isna(row.get("sensor_id")):
            continue
        rec: dict = {
            "room": str(row["sensor_id"]).lower(),
            "floor": int(float(row["floor"])) if pd.notna(row.get("floor")) else None,
            "timestamp": _parse_edt(row["datetime_edt"]),
            "temperature_f": round(float(row["temperature_f"]), 3),
            "temperature_c": round(float(row["temperature_c"]), 3),
            "humidity_pct": round(float(row["relative_humidity_pct"]), 3),
            "dew_point_f": round(float(row["dew_point_f"]), 3),
            "heat_index_f": round(float(row["heat_index_f"]), 3),
            "heat_index_c": round(float(row["heat_index_c"]), 3),
            "device_type": row.get("device_type"),
            "orientation": row.get("orientation"),
            "window_state": row.get("window_state"),
            "blinds_state": row.get("blinds_state"),
        }
        if pd.notna(row.get("wbgt_f")):
            rec["wbgt_f"] = round(float(row["wbgt_f"]), 3)
        if pd.notna(row.get("node_x")):
            rec["node_x"] = round(float(row["node_x"]), 4)
        if pd.notna(row.get("node_y")):
            rec["node_y"] = round(float(row["node_y"]), 4)
        if row.get("skipped"):
            rec["skipped"] = True
        records.append(rec)

    raw = json.dumps(records, default=str, indent=2)
    readings_path.write_text(re.sub(r": NaN", ": null", raw))
    readings_path.chmod(0o644)

    for old in output_dir.glob("readings_*.json"):
        if old != readings_path:
            old.unlink()

    manifest = {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "files": {"readings": browser_path},
    }
    with tempfile.NamedTemporaryFile(mode="w", dir=output_dir, suffix=".tmp", delete=False) as tf:
        tf.write(json.dumps(manifest, indent=2))
        tmp_name = tf.name
    Path(tmp_name).chmod(0o644)
    Path(tmp_name).rename(output_dir / "manifest.json")

    return readings_path, manifest


# ── Fill these in once the student provides the Dropbox folder structure ───────
_BASE = "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026"

PHASES: dict[str, dict] = {
    "phase1": {
        "dropbox_folder": f"{_BASE}/Phase 1 Archive/Latest",
        "config_path": f"{_BASE}/Phase 1 Archive/indoor_phase1_config.json",
        "output_dir": "./output/phase1",
        "browser_base": "/data/phase1",
        "start": "2026-06-03T17:00",  # actual deploy time
        "end": "2026-06-10T15:00",  # actual retrieve time; exclusive
        "exclude_dates": [],
    },
    "phase2": {
        "dropbox_folder": f"{_BASE}/Phase 2 Archive/Latest",
        "config_path": f"{_BASE}/Phase 2 Archive/indoor_phase2_config.json",
        "output_dir": "./output/phase2",
        "browser_base": "/data/phase2",
        "start": "2026-06-10T17:00",  # actual deploy time
        "end": "2026-06-15T15:00",  # actual retrieve time; exclusive
        "exclude_dates": [],
    },
    "phase3": {
        "dropbox_folder": f"{_BASE}/Phase 3 Archive/Latest",
        "config_path": f"{_BASE}/Phase 3 Archive/indoor_phase3_config.json",
        "output_dir": "./output/phase3",
        "browser_base": "/data/phase3",
        "start": "2026-06-15T17:00",  # actual deploy time
        "end": "2026-06-22T09:00",  # actual retrieve time; exclusive
        "exclude_dates": [],
    },
    "phase4": {
        "dropbox_folder": f"{_BASE}/Phase 4 Archive/Latest",
        "config_path": f"{_BASE}/Phase 4 Archive/indoor_phase4_config.json",
        "output_dir": "./output/phase4",
        "browser_base": "/data/phase4",
        "start": "2026-06-23T17:00",  # actual deploy time
        "end": "2026-06-25T09:00",  # actual retrieve time; exclusive
        "exclude_dates": [],
    },
    "heat_event": {
        "dropbox_folder": f"{_BASE}/Heat Event",
        "config_path": f"{_BASE}/Heat Event/indoor_heat_event_config.csv",
        "output_dir": "./output/heat_event",
        "browser_base": "/data/heat_event",
        "start": "2026-06-30T17:00",  # actual deploy time
        "end": "2026-07-06T14:00",  # actual retrieve time; exclusive
        "exclude_dates": [],
    },
}
# ──────────────────────────────────────────────────────────────────────────────


def _is_kestrel_file(filename: str) -> bool:
    """Distinguish Kestrel from HOBO when both start with a room number.

    HOBO:    '311 2026-07-06 14_26_14 EDT (Data EDT).csv'  → space after room#
    Kestrel: '311_Jul_6_2026_2_40_00_PM.csv'               → underscore after room#
    """
    m = re.match(r"^(\d+)(.)", Path(filename).stem)
    return bool(m and m.group(2) == "_")


def _parse_kestrel_filename(filename: str) -> dict:
    """Extract sensor_id (room number) from a Kestrel filename."""
    return {"sensor_id": Path(filename).stem.split("_")[0]}


def _read_kestrel_file(buf, meta: dict) -> pd.DataFrame:
    """Parse a Kestrel 5400 export (.xlsx or .csv) into the shared sensor DataFrame schema.

    Kestrel layout: rows 0-2 = device metadata, row 3 = column headers,
    row 4 = units, row 5+ = data. Temperatures are in °F; converted to °C
    so _normalize can treat all frames identically.
    """
    header = buf.read(4)
    buf.seek(0)
    is_xlsx = header[:2] == b"PK"
    if is_xlsx:
        df = pd.read_excel(buf, header=3, skiprows=[4], engine="openpyxl")
    else:
        df = pd.read_csv(buf, header=3, skiprows=[4])
    # Device occasionally emits corrupted/out-of-range tokens (e.g. "--------68.2----")
    # in place of a reading; coerce to NaN rather than letting them ride as strings
    # until they blow up the downstream groupby mean.
    temperature_f = pd.to_numeric(df["Temperature"], errors="coerce")
    dew_point_f = pd.to_numeric(df["Dew Point"], errors="coerce")
    out = pd.DataFrame({
        "sensor_id": meta["sensor_id"],
        "datetime_edt": pd.to_datetime(df["FORMATTED DATE_TIME"]),
        "temperature_c": (temperature_f - 32) * 5 / 9,
        "relative_humidity_pct": pd.to_numeric(df["Relative Humidity"], errors="coerce"),
        "dew_point_c": (dew_point_f - 32) * 5 / 9,
        "wbgt_f": pd.to_numeric(df["Wet Bulb Globe Temperature"], errors="coerce"),
        "device_type": "kestrel",
    })
    return out


def _load_heat_event_config(dropbox_res: DropboxResource, path: str) -> pd.DataFrame:
    """Load the heat-event config (CSV or Excel) keyed by room number."""
    buf = dropbox_res.download_file(path)
    cfg = pd.read_csv(buf) if path.endswith(".csv") else pd.read_excel(buf, engine="openpyxl")
    cfg.columns = cfg.columns.str.strip()
    return pd.DataFrame([
        {
            "sensor_id": str(int(r["Room#"])),
            "floor": int(r["Floor"]),
            "orientation": r["Orientation"],
            "window_state": r["Window State"],
            "blinds_state": r["Blinds State"],
            "fan": r["Fan"],
            "node_x": r.get("Node X"),
            "node_y": r.get("Node Y"),
            "skip_start": pd.to_datetime(r.get("Skip Start"), errors="coerce"),
            "skip_end": pd.to_datetime(r.get("Skip End"), errors="coerce"),
        }
        for _, r in cfg.iterrows()
    ])


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate, convert to °F, compute heat index, bin to 20-min intervals."""
    df = df.sort_values("last_update", ascending=False).drop_duplicates(
        subset=["sensor_id", "device_type", "datetime_edt"], keep="first"
    )
    df = df.copy()
    df["temperature_f"] = df["temperature_c"] * 9 / 5 + 32
    df["dew_point_f"] = df["dew_point_c"] * 9 / 5 + 32
    df["heat_index_f"] = _calculate_heat_index_f(df["temperature_f"], df["relative_humidity_pct"])
    df["heat_index_c"] = (df["heat_index_f"] - 32) * 5 / 9
    df["datetime_bin"] = df["datetime_edt"].dt.round("20min")
    agg: dict = dict(
        temperature_f=("temperature_f", "mean"),
        temperature_c=("temperature_c", "mean"),
        relative_humidity_pct=("relative_humidity_pct", "mean"),
        dew_point_f=("dew_point_f", "mean"),
        heat_index_f=("heat_index_f", "mean"),
        heat_index_c=("heat_index_c", "mean"),
    )
    if "wbgt_f" in df.columns:
        agg["wbgt_f"] = ("wbgt_f", "mean")
    return (
        df.groupby(["sensor_id", "device_type", "datetime_bin"])
        .agg(**agg)
        .reset_index()
        .rename(columns={"datetime_bin": "datetime_edt"})
    )


def run_phase(phase: str, dropbox_res: DropboxResource) -> None:
    cfg = PHASES[phase]

    all_files = dropbox_res.list_sensor_files(cfg["dropbox_folder"])
    if not all_files:
        raise SystemExit(f"No sensor files found in {cfg['dropbox_folder']!r}")

    frames = []
    for name, path in all_files:
        try:
            kestrel = _is_kestrel_file(name)
            if kestrel:
                meta = _parse_kestrel_filename(name)
                df = _read_kestrel_file(dropbox_res.download_file(path), meta)
            else:
                meta = _parse_sensor_filename(name)
                meta["source_file"] = name
                df = _read_sensor_file(dropbox_res.download_file(path), meta)
                df["device_type"] = "hobo"
            frames.append(df)
            print(f"  {'kestrel' if kestrel else 'hobo'} {name}: {len(df)} rows")
        except Exception as exc:
            print(f"  SKIP {name}: {exc}")

    if not frames:
        raise SystemExit("All files failed to parse — check output above")

    combined = pd.concat(frames, ignore_index=True)
    combined["last_update"] = datetime.now()

    aligned = _normalize(combined)
    config_path = cfg["config_path"]
    if config_path.endswith(".json"):
        config_df = _load_sensor_metadata(dropbox_res, config_path)
    else:
        config_df = _load_heat_event_config(dropbox_res, config_path)
    merged = aligned.merge(config_df, on="sensor_id", how="left")

    merged["skipped"] = (
        merged["skip_start"].notna() & merged["skip_end"].notna()
        & (merged["datetime_edt"] >= merged["skip_start"])
        & (merged["datetime_edt"] < merged["skip_end"])
    )

    # Clip to declared phase window
    start = pd.Timestamp(cfg["start"])
    end = pd.Timestamp(cfg["end"])
    merged = merged[(merged["datetime_edt"] >= start) & (merged["datetime_edt"] < end)]
    for excl in cfg.get("exclude_dates", []):
        excl_ts = pd.Timestamp(excl)
        merged = merged[~((merged["datetime_edt"] >= excl_ts) & (merged["datetime_edt"] < excl_ts + pd.Timedelta(days=1)))]
    print(f"  kept {len(merged)} rows after date filter ({cfg['start']} to {cfg['end']}, excluding {cfg.get('exclude_dates', [])})")

    output_dir = Path(cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "readings.csv"
    merged.to_csv(csv_path, index=False)
    print(f"  wrote {csv_path}")

    readings_path, manifest = _write_phase_export(
        output_dir, merged, datetime.utcnow(), browser_base=cfg["browser_base"]
    )
    print(f"  wrote {readings_path}")
    print(f"  manifest -> {manifest['files']['readings']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export one indoor-heat phase to JSON.")
    parser.add_argument("--phase", required=True, choices=list(PHASES), help="Phase to export")
    args = parser.parse_args()

    print(f"Exporting {args.phase}...")
    run_phase(args.phase, DropboxResource())
    print("Done.")


if __name__ == "__main__":
    main()
