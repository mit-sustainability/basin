import json
from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from dagster import Failure

from orchestrator.assets.outdoor_heat import (
    OutdoorSensorConfigPath,
    agol_outdoor_heat_sync,
    outdoor_heat_sensor_config,
)


def _make_combined_df() -> pd.DataFrame:
    return pd.DataFrame({
        "sensor_id": ["S1", "S2"],
        "datetime_edt": ["2026-06-01 12:00:00", "2026-06-01 12:20:00"],
        "temperature_f": [85.0, 87.0],
        "relative_humidity_pct": [60.0, 62.0],
        "dew_point_f": [69.0, 71.0],
        "heat_index_f": [90.0, 93.0],
        "sensor_name": ["Site A", "Site B"],
        "lat": [42.36, 42.37],
        "lon": [-71.09, -71.08],
        "deployment": ["Phase 1", "Phase 1"],
        "radiation_shield": [True, False],
    })


# ── outdoor_heat_sensor_config ────────────────────────────────────────────────

def test_outdoor_heat_sensor_config_loads_metadata():
    config_data = {
        "S1": {
            "name": "Site Alpha",
            "coords": [42.361, -71.097],
            "deployment": "Phase 1",
            "radiation_shield": True,
        },
        "S2": {
            "name": "Site Beta",
            "coords": [42.362, -71.098],
            "deployment": "Phase 2",
            "radiation_shield": False,
        },
    }
    mock_dropbox = MagicMock()
    mock_dropbox.download_file.return_value = BytesIO(json.dumps(config_data).encode())

    result = outdoor_heat_sensor_config(
        config=OutdoorSensorConfigPath(config_file_path="/path/config.json"),
        dropbox=mock_dropbox,
    )
    df = result.value
    assert len(df) == 2
    assert set(df.columns) == {"sensor_id", "sensor_name", "lat", "lon", "deployment", "radiation_shield"}
    assert df.loc[df["sensor_id"] == "S1", "sensor_name"].iloc[0] == "Site Alpha"
    assert df.loc[df["sensor_id"] == "S1", "lat"].iloc[0] == pytest.approx(42.361)
    assert df.loc[df["sensor_id"] == "S1", "lon"].iloc[0] == pytest.approx(-71.097)
    assert df.loc[df["sensor_id"] == "S2", "radiation_shield"].iloc[0] == False


# ── agol_outdoor_heat_sync ────────────────────────────────────────────────────

def test_agol_outdoor_heat_sync_raises_without_layer_url():
    with patch("orchestrator.assets.outdoor_heat._AGOL_LAYER_URL", ""):
        with pytest.raises(Failure, match="ARCGIS_OUTDOOR_HEAT_LAYER_URL"):
            agol_outdoor_heat_sync(pg_engine=MagicMock(), arcgis=MagicMock())


def test_agol_outdoor_heat_sync_calls_replace_features_with_geometry():
    mock_arcgis = MagicMock()
    mock_arcgis.replace_features.return_value = {"deleted": 10, "added": 12}
    mock_pg = MagicMock()

    with patch("orchestrator.assets.outdoor_heat._AGOL_LAYER_URL", "https://example.com/FeatureServer/0"):
        with patch("orchestrator.assets.outdoor_heat.pd.read_sql", return_value=_make_combined_df()):
            result = agol_outdoor_heat_sync(pg_engine=mock_pg, arcgis=mock_arcgis)

    mock_arcgis.replace_features.assert_called_once()
    call_kwargs = mock_arcgis.replace_features.call_args
    assert call_kwargs.kwargs["layer_url"] == "https://example.com/FeatureServer/0"
    assert call_kwargs.kwargs["geometry_fields"] == ("lat", "lon")


def test_agol_outdoor_heat_sync_returns_correct_metadata():
    mock_arcgis = MagicMock()
    mock_arcgis.replace_features.return_value = {"deleted": 10, "added": 12}
    mock_pg = MagicMock()

    with patch("orchestrator.assets.outdoor_heat._AGOL_LAYER_URL", "https://example.com/FeatureServer/0"):
        with patch("orchestrator.assets.outdoor_heat.pd.read_sql", return_value=_make_combined_df()):
            result = agol_outdoor_heat_sync(pg_engine=mock_pg, arcgis=mock_arcgis)

    assert result.metadata["features_deleted"].value == 10
    assert result.metadata["features_added"].value == 12
    assert result.metadata["total_rows_synced"].value == 2
