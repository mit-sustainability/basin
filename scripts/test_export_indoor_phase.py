import json
from datetime import datetime
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from scripts.export_indoor_phase import PHASES, run_phase


def _make_sensor_excel() -> BytesIO:
    df = pd.DataFrame({
        "#": [1, 2],
        "Date-Time (EDT)": ["05/15/2026 12:00:00", "05/15/2026 12:20:00"],
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
