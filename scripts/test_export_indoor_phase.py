import json
from datetime import datetime
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from scripts.export_indoor_phase import PHASES, _read_kestrel_file, run_phase


def _make_sensor_excel() -> BytesIO:
    df = pd.DataFrame({
        "#": [1, 2],
        "Date-Time (EDT)": ["06/04/2026 12:00:00", "06/04/2026 12:20:00"],
        "Temperature, °C": [21.96, 21.80],
        "RH, %": [41.92, 42.12],
        "Dew Point, °C": [8.45, 8.38],
    })
    buf = BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def _make_config_json() -> BytesIO:
    config = {
        "3": {
            "hobo_id": 21777605, "calibration_id": 1, "floor": 3,
            "orientation": "East", "window_state": "Closed 24/7",
            "blinds_state": "Open", "note": None,
            "sensor_photo": None, "window_photo": None,
            "node_x": 0.314, "node_y": 0.377,
        }
    }
    return BytesIO(json.dumps(config).encode())


def _mock_dropbox():
    dbx = MagicMock()
    dbx.list_sensor_files.return_value = [
        ("MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx",
         "/phase1/MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx"),
    ]

    def _fake_download(path):
        if path.endswith(".json"):
            return _make_config_json()
        return _make_sensor_excel()

    dbx.download_file.side_effect = _fake_download
    return dbx


def test_run_phase_writes_readings_and_manifest(tmp_path, monkeypatch):
    monkeypatch.setitem(PHASES, "phase1", {**PHASES["phase1"], "output_dir": str(tmp_path)})
    run_phase("phase1", _mock_dropbox())
    assert (tmp_path / "manifest.json").exists()
    readings = list(tmp_path.glob("readings_*.json"))
    assert len(readings) == 1


def test_run_phase_manifest_points_to_correct_browser_base(tmp_path, monkeypatch):
    monkeypatch.setitem(PHASES, "phase1", {**PHASES["phase1"], "output_dir": str(tmp_path)})
    run_phase("phase1", _mock_dropbox())
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["files"]["readings"].startswith("/data/phase1/")


def test_run_phase_readings_include_config_fields(tmp_path, monkeypatch):
    monkeypatch.setitem(PHASES, "phase1", {**PHASES["phase1"], "output_dir": str(tmp_path)})
    run_phase("phase1", _mock_dropbox())
    readings = json.loads(list(tmp_path.glob("readings_*.json"))[0].read_text())
    assert readings[0]["orientation"] == "East"
    assert readings[0]["floor"] == 3
    assert readings[0]["node_x"] == 0.314
    assert readings[0]["node_y"] == 0.377


def test_run_phase_readings_include_temperature_c(tmp_path, monkeypatch):
    monkeypatch.setitem(PHASES, "phase1", {**PHASES["phase1"], "output_dir": str(tmp_path)})
    run_phase("phase1", _mock_dropbox())
    readings = json.loads(list(tmp_path.glob("readings_*.json"))[0].read_text())
    assert readings[0]["temperature_c"] == pytest.approx(21.96, abs=0.01)
    csv_rows = pd.read_csv(tmp_path / "readings.csv")
    assert csv_rows.loc[0, "temperature_c"] == pytest.approx(21.96, abs=0.01)


def test_run_phase_readings_include_heat_index_c(tmp_path, monkeypatch):
    monkeypatch.setitem(PHASES, "phase1", {**PHASES["phase1"], "output_dir": str(tmp_path)})
    run_phase("phase1", _mock_dropbox())
    readings = json.loads(list(tmp_path.glob("readings_*.json"))[0].read_text())
    expected_c = (readings[0]["heat_index_f"] - 32) * 5 / 9
    assert readings[0]["heat_index_c"] == pytest.approx(expected_c, abs=0.01)
    csv_rows = pd.read_csv(tmp_path / "readings.csv")
    assert csv_rows.loc[0, "heat_index_c"] == pytest.approx(expected_c, abs=0.01)


def test_run_phase_flags_readings_inside_skip_window(tmp_path, monkeypatch):
    # Fixture's two readings are 06/04/2026 12:00:00 and 12:20:00 EDT.
    # Bracket only the first one with a skip window.
    config = {
        "3": {
            "floor": 3, "orientation": "East",
            "skip_start": "2026-06-04T11:00:00", "skip_end": "2026-06-04T12:10:00",
        }
    }
    dbx = _mock_dropbox()
    dbx.download_file.side_effect = lambda path: (
        BytesIO(json.dumps(config).encode()) if path.endswith(".json") else _make_sensor_excel()
    )
    monkeypatch.setitem(PHASES, "phase1", {**PHASES["phase1"], "output_dir": str(tmp_path)})
    run_phase("phase1", dbx)

    readings = json.loads(list(tmp_path.glob("readings_*.json"))[0].read_text())
    readings.sort(key=lambda r: r["timestamp"])
    assert readings[0]["skipped"] is True
    assert "skipped" not in readings[1]

    csv_rows = pd.read_csv(tmp_path / "readings.csv").sort_values("datetime_edt").reset_index(drop=True)
    assert bool(csv_rows.loc[0, "skipped"]) is True
    assert bool(csv_rows.loc[1, "skipped"]) is False


def test_read_kestrel_file_parses_plain_csv_export():
    # Real Kestrel exports observed in the field are plain CSV, not .xlsx,
    # despite the device name — this must not be routed through openpyxl.
    csv_text = (
        "KESTREL 5400 HEAT STRESS TRACKER\n"
        "Serial number: 1234567\n"
        "Device name: 311\n"
        "FORMATTED DATE_TIME,Temperature,Relative Humidity,Dew Point,Wet Bulb Globe Temperature\n"
        ",F,%,F,F\n"
        "07/06/2026 14:40:00,85.2,45.1,60.3,78.9\n"
    )
    buf = BytesIO(csv_text.encode())
    df = _read_kestrel_file(buf, {"sensor_id": "311"})
    assert len(df) == 1
    assert df.iloc[0]["sensor_id"] == "311"
    assert round(df.iloc[0]["temperature_c"], 1) == round((85.2 - 32) * 5 / 9, 1)


def test_read_kestrel_file_coerces_corrupted_readings_to_nan():
    # Kestrel devices occasionally emit out-of-range/corrupted tokens like
    # "--------68.2----" in place of a reading; these must become NaN, not
    # strings that later crash the groupby mean in _normalize.
    csv_text = (
        "KESTREL 5400 HEAT STRESS TRACKER\n"
        "Serial number: 1234567\n"
        "Device name: 311\n"
        "FORMATTED DATE_TIME,Temperature,Relative Humidity,Dew Point,Wet Bulb Globe Temperature\n"
        ",F,%,F,F\n"
        "07/06/2026 14:40:00,85.2,45.1,60.3,78.9\n"
        "07/06/2026 15:00:00,86.0,44.0,60.0,--------68.2----\n"
    )
    buf = BytesIO(csv_text.encode())
    df = _read_kestrel_file(buf, {"sensor_id": "311"})
    assert df["wbgt_f"].dtype.kind == "f"
    assert pd.isna(df.iloc[1]["wbgt_f"])
