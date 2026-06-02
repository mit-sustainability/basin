from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from dagster import Failure

from orchestrator.assets.indoor_heat import (
    IndoorHeatConfig,
    _parse_sensor_filename,
    _read_sensor_excel,
    raw_indoor_heat_sensor,
)


# ── filename parser ──────────────────────────────────────────────────────────

def test_parse_sensor_filename_extracts_location_and_sensor_id():
    result = _parse_sensor_filename("MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx")
    assert result["location"] == "MIT+Camb"
    assert result["sensor_id"] == "3"


def test_parse_sensor_filename_handles_multi_word_location():
    result = _parse_sensor_filename("MIT Main Campus 7 2026-05-15 14_04_50 EDT.xlsx")
    assert result["location"] == "MIT Main Campus"
    assert result["sensor_id"] == "7"


def test_parse_sensor_filename_raises_on_invalid_format():
    with pytest.raises(Failure):
        _parse_sensor_filename("nodate_nodatetime.xlsx")


# ── Excel reader ─────────────────────────────────────────────────────────────

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


def test_read_sensor_excel_standardizes_columns():
    meta = {"location": "MIT+Camb", "sensor_id": "3", "source_file": "test.xlsx"}
    result = _read_sensor_excel(_make_sensor_excel(), meta)
    assert list(result.columns) == [
        "row_num", "datetime_edt", "temperature_c",
        "relative_humidity_pct", "dew_point_c",
        "sensor_id", "location", "source_file",
    ]
    assert len(result) == 2
    assert result["sensor_id"].iloc[0] == "3"
    assert result["location"].iloc[0] == "MIT+Camb"


# ── raw_indoor_heat_sensor asset ─────────────────────────────────────────────

def test_raw_indoor_heat_sensor_skips_already_processed_files():
    mock_dropbox = MagicMock()
    mock_dropbox.list_excel_files.return_value = [
        ("MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx", "/folder/MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx"),
    ]
    mock_engine_resource = MagicMock()

    already_processed_df = pd.DataFrame(
        {"source_file": ["MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx"]}
    )
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=already_processed_df):
        result = raw_indoor_heat_sensor(
            config=IndoorHeatConfig(dropbox_folder="/folder"),
            dropbox=mock_dropbox,
            pg_engine=mock_engine_resource,
        )

    assert result.value.empty
    assert result.metadata["new_files"].value == 0


def test_raw_indoor_heat_sensor_raises_if_no_files_in_dropbox():
    mock_dropbox = MagicMock()
    mock_dropbox.list_excel_files.return_value = []
    mock_engine_resource = MagicMock()

    with pytest.raises(Failure, match="No Excel files found"):
        raw_indoor_heat_sensor(
            config=IndoorHeatConfig(dropbox_folder="/empty"),
            dropbox=mock_dropbox,
            pg_engine=mock_engine_resource,
        )
