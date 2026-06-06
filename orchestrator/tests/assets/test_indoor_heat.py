from io import BytesIO
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from dagster import Failure

from orchestrator.assets.indoor_heat import (
    IndoorHeatConfig,
    SensorConfigPath,
    _calculate_heat_index_f,
    _compute_calibration_stats,
    _load_sensor_metadata,
    _parse_sensor_filename,
    _read_sensor_file,
    indoor_heat_sensor,
    indoor_heat_sensor_config,
    stg_indoor_heat_aligned,
)

# ── filename parser ──────────────────────────────────────────────────────────

def test_parse_sensor_filename_extracts_sensor_id():
    result = _parse_sensor_filename("MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx")
    assert result["sensor_id"] == "3"


def test_parse_sensor_filename_handles_multi_token_prefix():
    result = _parse_sensor_filename("MIT Main Campus 7 2026-05-15 14_04_50 EDT.xlsx")
    assert result["sensor_id"] == "7"


def test_parse_sensor_filename_raises_on_invalid_format():
    with pytest.raises(Failure):
        _parse_sensor_filename("nodate_nodatetime.xlsx")


def test_parse_sensor_filename_handles_no_location_prefix():
    result = _parse_sensor_filename("22086523 2025-11-24 13_46_22 EST (Data EST).xlsx")
    assert result["sensor_id"] == "22086523"


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
    meta = {"sensor_id": "3", "source_file": "test.xlsx"}
    result = _read_sensor_file(_make_sensor_excel(), meta)
    assert list(result.columns) == [
        "row_num", "datetime_edt", "temperature_c",
        "relative_humidity_pct", "dew_point_c",
        "sensor_id", "source_file",
    ]
    assert len(result) == 2
    assert result["sensor_id"].iloc[0] == "3"


# ── indoor_heat_sensor asset ─────────────────────────────────────────────────

def test_indoor_heat_sensor_processes_all_files():
    mock_dropbox = MagicMock()
    mock_dropbox.list_sensor_files.return_value = [
        ("MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx", "/folder/MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx"),
    ]
    mock_dropbox.download_file.return_value = _make_sensor_excel()

    result = indoor_heat_sensor(
        config=IndoorHeatConfig(dropbox_folder="/folder"),
        dropbox=mock_dropbox,
    )

    assert len(result.value) == 2
    assert result.metadata["total_files"].value == 1


def test_indoor_heat_sensor_raises_if_no_files_in_dropbox():
    mock_dropbox = MagicMock()
    mock_dropbox.list_sensor_files.return_value = []

    with pytest.raises(Failure, match="No sensor files found"):
        indoor_heat_sensor(
            config=IndoorHeatConfig(dropbox_folder="/empty"),
            dropbox=mock_dropbox,
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
    meta = {"sensor_id": "3", "source_file": "test.xlsx"}
    result = _read_sensor_file(_make_celsius_space_variant_excel(), meta)
    assert "temperature_c" in result.columns
    assert result["temperature_c"].iloc[0] == pytest.approx(21.96, abs=0.01)


def test_read_sensor_file_converts_fahrenheit_to_celsius():
    meta = {"sensor_id": "3", "source_file": "test.xlsx"}
    result = _read_sensor_file(_make_fahrenheit_excel(), meta)
    assert "temperature_c" in result.columns
    assert "temp_f_raw" not in result.columns
    assert result["temperature_c"].iloc[0] == pytest.approx(21.96, abs=0.05)


def test_read_sensor_file_handles_csv():
    meta = {"sensor_id": "5", "source_file": "test.csv"}
    result = _read_sensor_file(_make_sensor_csv(), meta)
    assert len(result) == 2
    assert result["temperature_c"].iloc[0] == pytest.approx(21.96, abs=0.01)


def test_read_sensor_file_fills_missing_dew_point_with_nan():
    meta = {"sensor_id": "1", "source_file": "nodew.xlsx"}
    result = _read_sensor_file(_make_no_dew_point_excel(), meta)
    assert "dew_point_c" in result.columns
    assert pd.isna(result["dew_point_c"].iloc[0])


def test_read_sensor_file_generates_row_num_when_hash_absent():
    meta = {"sensor_id": "1", "source_file": "nodew.xlsx"}
    result = _read_sensor_file(_make_no_dew_point_excel(), meta)
    assert "row_num" in result.columns
    assert result["row_num"].iloc[0] == 0


# ── heat index helper ────────────────────────────────────────────────────────

def test_calculate_heat_index_f_uses_simple_estimate_below_80():
    # At 70°F / 50% RH: hi_simple = 0.5*(70+61+(70-68)*1.2+50*0.094) = 69.05; avg with 70 = 69.525
    result = _calculate_heat_index_f(pd.Series([70.0]), pd.Series([50.0]))
    assert result.iloc[0] == pytest.approx(69.525, abs=0.1)


def test_calculate_heat_index_f_uses_rothfusz_above_80():
    # At 90°F / 50% RH: NOAA reference ≈ 95°F
    result = _calculate_heat_index_f(pd.Series([90.0]), pd.Series([50.0]))
    assert result.iloc[0] == pytest.approx(95.0, abs=1.0)


# ── indoor_heat_sensor_config asset ──────────────────────────────────────────

def test_indoor_heat_sensor_config_loads_metadata():
    import json as _json
    config_data = {
        "304": {
            "hobo_id": 21777605, "calibration_id": 6, "floor": 7,
            "orientation": "East", "window_state": "Closed 24/7",
            "blinds_state": "Open", "note": None,
            "sensor_photo": "704 sensor.HEIC", "window_photo": "704 windows.HEIC",
        },
        "305": {
            "hobo_id": 21777606, "calibration_id": 7, "floor": 3,
            "orientation": "West", "window_state": "Open", "blinds_state": "Closed",
            "note": "near HVAC vent", "sensor_photo": None, "window_photo": None,
        },
    }
    mock_dropbox = MagicMock()
    mock_dropbox.download_file.return_value = BytesIO(_json.dumps(config_data).encode())
    result = indoor_heat_sensor_config(
        config=SensorConfigPath(config_file_path="/test/indoor_sensor_config.json"),
        dropbox=mock_dropbox,
    )
    df = result.value
    assert len(df) == 2
    assert set(df.columns) == {
        "sensor_id", "hobo_id", "calibration_id", "floor",
        "orientation", "window_state", "blinds_state", "note",
        "sensor_photo", "window_photo",
    }
    assert df.loc[df["sensor_id"] == "304", "floor"].iloc[0] == 7
    assert df.loc[df["sensor_id"] == "305", "orientation"].iloc[0] == "West"


# ── stg_indoor_heat_aligned asset (dedup + °F + 20-min bins) ─────────────────

def _make_raw_sql_df() -> pd.DataFrame:
    """Raw sensor readings with two readings per sensor, in °C."""
    return pd.DataFrame({
        "sensor_id": ["A", "A", "B", "B"],
        "datetime_edt": pd.to_datetime([
            "2026-05-15 12:00:00",
            "2026-05-15 12:05:00",
            "2026-05-15 12:00:00",
            "2026-05-15 12:20:00",
        ]),
        "temperature_c": [21.96, 21.80, 22.0, 22.5],
        "relative_humidity_pct": [41.92, 42.12, 40.0, 41.0],
        "dew_point_c": [8.45, 8.38, 8.0, 8.2],
        "source_file": ["a.xlsx"] * 4,
        "last_update": pd.to_datetime(["2026-05-15"] * 4),
        "row_num": [1, 2, 3, 4],
    })


def test_stg_indoor_heat_aligned_temperature_f_conversion():
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=_make_raw_sql_df()):
        result = stg_indoor_heat_aligned(pg_engine=MagicMock())
    df = result.value
    sensor_b_20 = df[(df["sensor_id"] == "B") & (df["datetime_edt"] == pd.Timestamp("2026-05-15 12:20:00"))].iloc[0]
    assert sensor_b_20["temperature_f"] == pytest.approx(22.5 * 9/5 + 32, abs=0.01)


def test_stg_indoor_heat_aligned_collapses_readings_into_20min_bins():
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=_make_raw_sql_df()):
        result = stg_indoor_heat_aligned(pg_engine=MagicMock())
    df = result.value
    # Sensor A: 12:00 and 12:05 collapse to 12:00 bin → 1 row
    # Sensor B: 12:00 and 12:20 are separate bins → 2 rows
    # Total: 3 rows
    assert len(df) == 3


def test_stg_indoor_heat_aligned_averages_readings_in_bin():
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=_make_raw_sql_df()):
        result = stg_indoor_heat_aligned(pg_engine=MagicMock())
    df = result.value
    sensor_a = df[df["sensor_id"] == "A"].iloc[0]
    # avg(21.96, 21.80)°C = 21.88°C → 71.384°F; avg RH = (41.92+42.12)/2 = 42.02
    assert sensor_a["temperature_f"] == pytest.approx(21.88 * 9/5 + 32, abs=0.01)
    assert sensor_a["relative_humidity_pct"] == pytest.approx(42.02, abs=0.01)


def test_stg_indoor_heat_aligned_output_columns():
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=_make_raw_sql_df()):
        result = stg_indoor_heat_aligned(pg_engine=MagicMock())
    assert set(result.value.columns) == {
        "sensor_id", "datetime_edt",
        "temperature_f", "relative_humidity_pct", "dew_point_f", "heat_index_f",
    }


# ── _compute_calibration_stats ───────────────────────────────────────────────

_CALIB_VARS = ["temperature_f", "relative_humidity_pct", "heat_index_f"]


def _make_aligned_df_6_sensors() -> pd.DataFrame:
    """5 normal sensors at 70°F + 1 outlier at 90°F.
    With 6 sensors: n_sigma for outlier = 5/sqrt(6) ≈ 2.04 → marginal outlier.
    """
    times = pd.date_range("2026-05-15", periods=5, freq="20min")
    dfs = []
    for sid in ["A", "B", "C", "D", "E"]:
        dfs.append(pd.DataFrame({
            "sensor_id": sid,
            "datetime_edt": times,
            "temperature_f": 70.0,
            "relative_humidity_pct": 50.0,
            "heat_index_f": 70.0,
        }))
    dfs.append(pd.DataFrame({
        "sensor_id": "F",
        "datetime_edt": times,
        "temperature_f": 90.0,
        "relative_humidity_pct": 50.0,
        "heat_index_f": 90.0,
    }))
    return pd.concat(dfs, ignore_index=True)


def _make_aligned_df_12_sensors() -> pd.DataFrame:
    """11 normal sensors at 70°F + 1 outlier at 90°F.
    With 12 sensors: n_sigma for outlier = 11/sqrt(12) ≈ 3.18 → excluded.
    """
    times = pd.date_range("2026-05-15", periods=5, freq="20min")
    dfs = []
    for i in range(11):
        dfs.append(pd.DataFrame({
            "sensor_id": f"S{i:02d}",
            "datetime_edt": times,
            "temperature_f": 70.0,
            "relative_humidity_pct": 50.0,
            "heat_index_f": 70.0,
        }))
    dfs.append(pd.DataFrame({
        "sensor_id": "OUTLIER",
        "datetime_edt": times,
        "temperature_f": 90.0,
        "relative_humidity_pct": 50.0,
        "heat_index_f": 90.0,
    }))
    return pd.concat(dfs, ignore_index=True)


def test_compute_calibration_stats_detects_marginal_outlier():
    _, sensor_stats = _compute_calibration_stats(_make_aligned_df_6_sensors())
    sensor_f = sensor_stats[sensor_stats["sensor_id"] == "F"].iloc[0]
    assert sensor_f["is_outlier"] is True or sensor_f["is_outlier"] == True
    assert sensor_f["severity"] == "marginal"


def test_compute_calibration_stats_passing_sensors_not_flagged():
    _, sensor_stats = _compute_calibration_stats(_make_aligned_df_6_sensors())
    passing = sensor_stats[sensor_stats["sensor_id"].isin(["A", "B", "C", "D", "E"])]
    assert (passing["severity"] == "pass").all()
    assert (~passing["is_outlier"]).all()


def test_compute_calibration_stats_detects_excluded_outlier():
    _, sensor_stats = _compute_calibration_stats(_make_aligned_df_12_sensors())
    outlier = sensor_stats[sensor_stats["sensor_id"] == "OUTLIER"].iloc[0]
    assert outlier["severity"] == "excluded"
