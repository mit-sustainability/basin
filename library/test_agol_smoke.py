"""Smoke test for ArcGIS Online replace_features.

Pushes a single sample row to the outdoor heat feature layer to verify:
- OAuth2 credentials work
- Layer URL is correct and app has edit permissions
- Feature layer column schema matches

WARNING: deleteFeatures runs first — use a scratch layer if production data
must be preserved. To test auth only, call arcgis._get_token() instead.

Usage:
    python library/test_agol_smoke.py
"""
import os

import pandas as pd

from orchestrator.resources.arcgis import ArcGISResource

arcgis = ArcGISResource(
    client_id=os.environ["ARCGIS_CLIENT_ID"],
    client_secret=os.environ["ARCGIS_CLIENT_SECRET"],
    org_url=os.environ.get("ARCGIS_ORG_URL", "https://www.arcgis.com"),
)

sample = pd.DataFrame([{
    "sensor_id": "S21",
    "datetime_edt": "2026-06-16T12:00:00",
    "temperature_f": 85.0,
    "relative_humidity_pct": 55.0,
    "dew_point_f": 67.0,
    "heat_index_f": 88.0,
    "sensor_name": "Eastman Court",
    "lat": 42.3602861,
    "lon": -71.0902504,
    "deployment": "Zone 4",
    "radiation_shield": None,
}])

result = arcgis.replace_features(
    layer_url=os.environ["ARCGIS_OUTDOOR_HEAT_LAYER_URL"],
    df=sample,
    geometry_fields=("lat", "lon"),
)
print(result)
