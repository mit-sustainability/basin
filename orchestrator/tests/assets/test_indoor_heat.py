from io import BytesIO
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from dagster import Failure

from orchestrator.assets.indoor_heat import (
    IndoorHeatConfig,
    _calculate_heat_index_f,
    _parse_sensor_filename,
    _read_sensor_file,          # renamed from _read_sensor_excel
    raw_indoor_heat_sensor,
    stg_indoor_heat,
)
from orchestrator.resources.dropbox import DropboxResource


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


def test_parse_sensor_filename_handles_csv_extension():
    result = _parse_sensor_filename("MIT+Camb 3 2026-05-15 14_04_50 EDT.csv")
    assert result["location"] == "MIT+Camb"
    assert result["sensor_id"] == "3"


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


def test_read_sensor_file_standardizes_columns():
    meta = {"location": "MIT+Camb", "sensor_id": "3", "source_file": "test.xlsx"}
    result = _read_sensor_file(_make_sensor_excel(), meta)
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
    mock_dropbox.list_sensor_files.return_value = [
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
    mock_dropbox.list_sensor_files.return_value = []
    mock_engine_resource = MagicMock()

    with pytest.raises(Failure, match="No sensor files found"):
        raw_indoor_heat_sensor(
            config=IndoorHeatConfig(dropbox_folder="/empty"),
            dropbox=mock_dropbox,
            pg_engine=mock_engine_resource,
        )


def _make_celsius_space_variant_excel() -> BytesIO:
    """Simulates HOBO export with space-before-comma column names."""
    df = pd.DataFrame({
        "#": [1, 2],
        "Date-Time (EDT)": ["05/15/2026 12:00:00", "05/15/2026 12:20:00"],
        "Temperature , °C": [21.96, 21.80],
        "RH , %": [41.92, 42.12],
        "Dew Point , °C": [8.45, 8.38],
    })
    buf = BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def _make_fahrenheit_excel() -> BytesIO:
    """Simulates HOBO export with °F columns."""
    df = pd.DataFrame({
        "#": [1, 2],
        "Date-Time (EDT)": ["05/15/2026 12:00:00", "05/15/2026 12:20:00"],
        "Temperature , °F": [71.528, 71.24],
        "RH , %": [41.92, 42.12],
        "Dew Point , °F": [47.21, 47.08],
    })
    buf = BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def _make_sensor_csv() -> BytesIO:
    content = (
        'Date-Time (EST),"temp , °C","rh , %"\n'
        "05/15/2026 12:00:00,21.96,41.92\n"
        "05/15/2026 12:20:00,21.80,42.12\n"
    )
    return BytesIO(content.encode())


def _make_no_dew_point_excel() -> BytesIO:
    df = pd.DataFrame({
        "Date-Time (EDT)": ["05/15/2026 12:00:00"],
        "Temperature, °C": [21.96],
        "RH, %": [41.92],
    })
    buf = BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def test_read_sensor_file_handles_celsius_space_variants():
    meta = {"location": "MIT+Camb", "sensor_id": "3", "source_file": "test.xlsx"}
    result = _read_sensor_file(_make_celsius_space_variant_excel(), meta)
    assert "temperature_c" in result.columns
    assert result["temperature_c"].iloc[0] == pytest.approx(21.96, abs=0.01)


def test_read_sensor_file_converts_fahrenheit_to_celsius():
    meta = {"location": "MIT+Camb", "sensor_id": "3", "source_file": "test.xlsx"}
    result = _read_sensor_file(_make_fahrenheit_excel(), meta)
    assert "temperature_c" in result.columns
    assert "temp_f_raw" not in result.columns
    assert result["temperature_c"].iloc[0] == pytest.approx(21.96, abs=0.05)


def test_read_sensor_file_handles_csv():
    meta = {"location": "MIT+Camb", "sensor_id": "5", "source_file": "test.csv"}
    result = _read_sensor_file(_make_sensor_csv(), meta)
    assert len(result) == 2
    assert result["temperature_c"].iloc[0] == pytest.approx(21.96, abs=0.01)


def test_read_sensor_file_fills_missing_dew_point_with_nan():
    meta = {"location": "MIT", "sensor_id": "1", "source_file": "nodew.xlsx"}
    result = _read_sensor_file(_make_no_dew_point_excel(), meta)
    assert "dew_point_c" in result.columns
    assert pd.isna(result["dew_point_c"].iloc[0])


def test_read_sensor_file_generates_row_num_when_hash_absent():
    meta = {"location": "MIT", "sensor_id": "1", "source_file": "nodew.xlsx"}
    result = _read_sensor_file(_make_no_dew_point_excel(), meta)
    assert "row_num" in result.columns
    assert result["row_num"].iloc[0] == 0


def _make_dropbox_entries(names: list[str]) -> list:
    entries = []
    for name in names:
        e = MagicMock()
        e.name = name
        e.path_lower = f"/folder/{name}"
        entries.append(e)
    return entries


def test_list_sensor_files_includes_xlsx_xls_and_csv():
    names = ["sensor.xlsx", "sensor.xls", "sensor.csv", "notes.txt", "~$sensor.xlsx"]
    mock_result = MagicMock()
    mock_result.entries = _make_dropbox_entries(names)
    mock_result.has_more = False

    mock_dbx = MagicMock()
    mock_dbx.files_list_folder.return_value = mock_result

    with patch("orchestrator.resources.dropbox.dropbox.Dropbox", return_value=mock_dbx):
        resource = DropboxResource(access_token="test")
        files = resource.list_sensor_files("/folder")

    returned_names = [f[0] for f in files]
    assert "sensor.xlsx" in returned_names
    assert "sensor.xls" in returned_names
    assert "sensor.csv" in returned_names
    assert "notes.txt" not in returned_names
    assert "~$sensor.xlsx" in returned_names  # lock files are not filtered; caller is responsible


def test_list_sensor_files_paginates():
    result1 = MagicMock()
    result1.entries = _make_dropbox_entries(["a.xlsx"])
    result1.has_more = True
    result1.cursor = "cursor1"

    result2 = MagicMock()
    result2.entries = _make_dropbox_entries(["b.csv"])
    result2.has_more = False

    mock_dbx = MagicMock()
    mock_dbx.files_list_folder.return_value = result1
    mock_dbx.files_list_folder_continue.return_value = result2

    with patch("orchestrator.resources.dropbox.dropbox.Dropbox", return_value=mock_dbx):
        resource = DropboxResource(access_token="test")
        files = resource.list_sensor_files("/folder")

    assert {f[0] for f in files} == {"a.xlsx", "b.csv"}


# ── heat index helper ────────────────────────────────────────────────────────

def test_calculate_heat_index_f_uses_simple_estimate_below_80():
    # At 70°F / 50% RH: hi_simple = 0.5*(70+61+(70-68)*1.2+50*0.094) = 69.05; avg with 70 = 69.525
    result = _calculate_heat_index_f(pd.Series([70.0]), pd.Series([50.0]))
    assert result.iloc[0] == pytest.approx(69.525, abs=0.1)


def test_calculate_heat_index_f_uses_rothfusz_above_80():
    # At 90°F / 50% RH: NOAA reference ≈ 95°F
    result = _calculate_heat_index_f(pd.Series([90.0]), pd.Series([50.0]))
    assert result.iloc[0] == pytest.approx(95.0, abs=1.0)


# ── stg_indoor_heat asset ────────────────────────────────────────────────────

def _make_raw_sql_df() -> pd.DataFrame:
    return pd.DataFrame({
        "sensor_id": ["3", "3"],
        "location": ["MIT+Camb", "MIT+Camb"],
        "datetime_edt": pd.to_datetime(["2026-05-15 12:00:00", "2026-05-15 12:20:00"]),
        "temperature_c": [21.96, 21.80],
        "relative_humidity_pct": [41.92, 42.12],
        "dew_point_c": [8.45, 8.38],
        "source_file": ["test.xlsx", "test.xlsx"],
        "last_update": pd.to_datetime(["2026-05-15", "2026-05-15"]),
        "row_num": [1, 2],
    })


def test_stg_indoor_heat_outputs_fahrenheit_columns():
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=_make_raw_sql_df()):
        result = stg_indoor_heat(pg_engine=MagicMock())
    df = result.value
    assert "temperature_f" in df.columns
    assert "dew_point_f" in df.columns
    assert "heat_index_f" in df.columns
    assert "temperature_c" not in df.columns
    assert "dew_point_c" not in df.columns


def test_stg_indoor_heat_temperature_f_conversion():
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=_make_raw_sql_df()):
        result = stg_indoor_heat(pg_engine=MagicMock())
    df = result.value
    expected_f = 21.96 * 9 / 5 + 32  # 71.528°F
    assert df["temperature_f"].iloc[0] == pytest.approx(expected_f, abs=0.01)


def test_stg_indoor_heat_deduplicates():
    duplicate = pd.concat([_make_raw_sql_df(), _make_raw_sql_df()], ignore_index=True)
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=duplicate):
        result = stg_indoor_heat(pg_engine=MagicMock())
    assert len(result.value) == 2
