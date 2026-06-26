"""Export one indoor-heat phase to JSON + manifest.

Usage:
    python scripts/export_indoor_phase.py --phase phase1

Fill in PHASES below with the Dropbox paths the student provides for each phase.
"""

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from orchestrator.assets.indoor_heat import (
    _calculate_heat_index_f,
    _load_sensor_metadata,
    _parse_sensor_filename,
    _read_sensor_file,
    _write_heat_export,
)
from orchestrator.resources.dropbox import DropboxResource

# ── Fill these in once the student provides the Dropbox folder structure ───────
PHASES: dict[str, dict] = {
    "phase1": {
        "dropbox_folder": "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase1",
        "config_path":    "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase1/sensor_config.json",
        "output_dir":     "./output/phase1",
        "browser_base":   "/data/phase1",
    },
    "phase2": {
        "dropbox_folder": "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase2",
        "config_path":    "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase2/sensor_config.json",
        "output_dir":     "./output/phase2",
        "browser_base":   "/data/phase2",
    },
    "phase3": {
        "dropbox_folder": "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase3",
        "config_path":    "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase3/sensor_config.json",
        "output_dir":     "./output/phase3",
        "browser_base":   "/data/phase3",
    },
    "phase4": {
        "dropbox_folder": "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase4",
        "config_path":    "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase4/sensor_config.json",
        "output_dir":     "./output/phase4",
        "browser_base":   "/data/phase4",
    },
}
# ──────────────────────────────────────────────────────────────────────────────


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate, convert to °F, compute heat index, bin to 20-min intervals."""
    df = df.sort_values("last_update", ascending=False).drop_duplicates(
        subset=["sensor_id", "datetime_edt"], keep="first"
    )
    df = df.copy()
    df["temperature_f"] = df["temperature_c"] * 9 / 5 + 32
    df["dew_point_f"] = df["dew_point_c"] * 9 / 5 + 32
    df["heat_index_f"] = _calculate_heat_index_f(df["temperature_f"], df["relative_humidity_pct"])
    df["datetime_bin"] = df["datetime_edt"].dt.round("20min")
    return (
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


def run_phase(phase: str, dropbox: DropboxResource) -> None:
    cfg = PHASES[phase]

    all_files = dropbox.list_sensor_files(cfg["dropbox_folder"])
    if not all_files:
        raise SystemExit(f"No sensor files found in {cfg['dropbox_folder']!r}")

    frames = []
    for name, path in all_files:
        try:
            meta = _parse_sensor_filename(name)
            meta["source_file"] = name
            df = _read_sensor_file(dropbox.download_file(path), meta)
            frames.append(df)
            print(f"  {name}: {len(df)} rows")
        except Exception as exc:
            print(f"  SKIP {name}: {exc}")

    if not frames:
        raise SystemExit("All files failed to parse — check output above")

    combined = pd.concat(frames, ignore_index=True)
    combined["last_update"] = datetime.now()

    aligned = _normalize(combined)
    config_df = _load_sensor_metadata(dropbox, cfg["config_path"])
    merged = aligned.merge(config_df, on="sensor_id", how="left")

    output_dir = Path(cfg["output_dir"])
    readings_path, manifest = _write_heat_export(
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
