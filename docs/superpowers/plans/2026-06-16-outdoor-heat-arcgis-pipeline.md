# Outdoor Heat ArcGIS Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adapt the indoor heat pipeline for outdoor sensors, replacing the JSON export with a full-replace push to an ArcGIS Online hosted feature layer that views and dashboards consume automatically.

**Architecture:** A new dbt model `final_outdoor_heat_combined` joins 20-min aligned outdoor sensor readings with config metadata (lat/lon/name). A new `replace_features` method on `ArcGISResource` deletes all existing features then adds all rows in one pass. The `agol_outdoor_heat_sync` Dagster asset reads from the final model and calls `replace_features` with point geometry. The job is scheduled weekly on Sunday midnight UTC.

**Tech Stack:** Dagster 1.12, dbt-postgres 1.10, ArcGIS Online REST API (OAuth2 client_credentials), pandas 2.2, pytest, uv

**All commands run from:** `/Users/yucheng/Documents/Projects/basin`

---

## File Map

| Action | Path |
|---|---|
| Create | `warehouse/models/final/final_outdoor_heat_combined.sql` |
| Modify | `warehouse/models/final/schema.yml` |
| Modify | `orchestrator/resources/arcgis.py` |
| Modify | `orchestrator/tests/resources/test_arcgis.py` |
| Modify | `orchestrator/assets/outdoor_heat.py` |
| Create | `orchestrator/tests/assets/test_outdoor_heat.py` |
| Modify | `orchestrator/jobs/outdoor_heat_job.py` |
| Modify | `orchestrator/schedules/mitos_warehouse.py` |

---

## Task 1: dbt model `final_outdoor_heat_combined`

**Files:**
- Create: `warehouse/models/final/final_outdoor_heat_combined.sql`
- Modify: `warehouse/models/final/schema.yml`

- [ ] **Step 1: Create the SQL model**

Create `warehouse/models/final/final_outdoor_heat_combined.sql`:

```sql
SELECT
    a.sensor_id,
    a.datetime_edt,
    a.temperature_f,
    a.relative_humidity_pct,
    a.dew_point_f,
    a.heat_index_f,
    c.sensor_name,
    c.lat,
    c.lon,
    c.deployment,
    c.radiation_shield
FROM {{ source("staging", "stg_outdoor_heat_aligned") }} a
LEFT JOIN {{ source("raw", "outdoor_heat_sensor_config") }} c
    ON a.sensor_id = c.sensor_id
```

- [ ] **Step 2: Add schema entry**

In `warehouse/models/final/schema.yml`, insert after the `outdoor_heat_daily_summary` block (around line 450) and before the `indoor_heat_daily_summary` block:

```yaml
  - name: final_outdoor_heat_combined
    description: >
      Granular 20-minute sensor readings joined with sensor metadata (lat/lon/name).
      Combines staging.stg_outdoor_heat_aligned with raw.outdoor_heat_sensor_config
      for use by the ArcGIS Online outdoor heat export.
    meta:
      owner: yu_cheng@mit.edu
    columns:
      - name: sensor_id
        description: "Sensor identifier"
      - name: datetime_edt
        description: "Timestamp of the 20-minute reading bin, in EDT"
      - name: temperature_f
        description: "Average temperature in °F for the bin"
      - name: relative_humidity_pct
        description: "Average relative humidity percentage for the bin"
      - name: dew_point_f
        description: "Average dew point in °F for the bin"
      - name: heat_index_f
        description: "NOAA/Rothfusz heat index in °F for the bin"
      - name: sensor_name
        description: "Human-readable site name"
      - name: lat
        description: "Latitude of sensor deployment location"
      - name: lon
        description: "Longitude of sensor deployment location"
      - name: deployment
        description: "Deployment phase (e.g. Phase 1, Phase 2)"
      - name: radiation_shield
        description: "Whether the sensor has a radiation shield fitted"
```

- [ ] **Step 3: Verify dbt compiles**

```bash
uv run dbt compile --project-dir warehouse --profiles-dir warehouse --select final_outdoor_heat_combined
```

Expected: `Completed successfully` with no errors.

- [ ] **Step 4: Commit**

```bash
git add warehouse/models/final/final_outdoor_heat_combined.sql warehouse/models/final/schema.yml
git commit -m "feat: add final_outdoor_heat_combined dbt model"
```

---

## Task 2: `ArcGISResource.replace_features` (TDD)

**Files:**
- Modify: `orchestrator/resources/arcgis.py`
- Modify: `orchestrator/tests/resources/test_arcgis.py`

- [ ] **Step 1: Write the failing tests**

Append a new `TestReplaceFeatures` class to `orchestrator/tests/resources/test_arcgis.py`:

```python
class TestReplaceFeatures:
    def _route(self, token_resp, delete_resp, add_resp):
        """Return a side_effect function that routes POST calls by URL."""
        def mock_post(url, data=None, **kwargs):
            if url.endswith("/token"):
                return token_resp
            elif url.endswith("/deleteFeatures"):
                return delete_resp
            else:
                return add_resp
        return mock_post

    def _token_resp(self):
        r = MagicMock()
        r.json.return_value = {"access_token": "tok"}
        return r

    def test_deletes_all_then_adds_all(self, resource):
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"deleteResults": [{"success": True}, {"success": True}]}
        add_resp = MagicMock()
        add_resp.json.return_value = {"addResults": [{"success": True}, {"success": True}]}

        df = pd.DataFrame([
            {"sensor_id": "A", "temperature_f": 80.0},
            {"sensor_id": "B", "temperature_f": 85.0},
        ])

        with patch("orchestrator.resources.arcgis.requests.post",
                   side_effect=self._route(self._token_resp(), delete_resp, add_resp)):
            result = resource.replace_features(
                layer_url="https://example.com/FeatureServer/0",
                df=df,
            )

        assert result["deleted"] == 2
        assert result["added"] == 2

    def test_attaches_point_geometry_when_geometry_fields_set(self, resource):
        import json as _json
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"deleteResults": []}
        add_resp = MagicMock()
        add_resp.json.return_value = {"addResults": [{"success": True}]}

        post_calls = []

        def routing_post(url, data=None, **kwargs):
            post_calls.append((url, data))
            if url.endswith("/token"):
                return self._token_resp()
            elif url.endswith("/deleteFeatures"):
                return delete_resp
            return add_resp

        df = pd.DataFrame([{"sensor_id": "S1", "lat": 42.36, "lon": -71.09, "temperature_f": 80.0}])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=routing_post):
            resource.replace_features(
                layer_url="https://example.com/FeatureServer/0",
                df=df,
                geometry_fields=("lat", "lon"),
            )

        add_data = next(data for url, data in post_calls if "addFeatures" in url)
        features = _json.loads(add_data["features"])
        assert features[0]["geometry"]["x"] == pytest.approx(-71.09)
        assert features[0]["geometry"]["y"] == pytest.approx(42.36)
        assert features[0]["geometry"]["spatialReference"]["wkid"] == 4326

    def test_no_geometry_when_geometry_fields_none(self, resource):
        import json as _json
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"deleteResults": []}
        add_resp = MagicMock()
        add_resp.json.return_value = {"addResults": [{"success": True}]}

        post_calls = []

        def routing_post(url, data=None, **kwargs):
            post_calls.append((url, data))
            if url.endswith("/token"):
                return self._token_resp()
            elif url.endswith("/deleteFeatures"):
                return delete_resp
            return add_resp

        df = pd.DataFrame([{"sensor_id": "S1", "temperature_f": 80.0}])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=routing_post):
            resource.replace_features(
                layer_url="https://example.com/FeatureServer/0",
                df=df,
                geometry_fields=None,
            )

        add_data = next(data for url, data in post_calls if "addFeatures" in url)
        features = _json.loads(add_data["features"])
        assert "geometry" not in features[0]

    def test_raises_on_delete_error(self, resource):
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"error": {"code": 500, "message": "Server error"}}

        def routing_post(url, data=None, **kwargs):
            if url.endswith("/token"):
                return self._token_resp()
            return delete_resp

        df = pd.DataFrame([{"sensor_id": "A"}])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=routing_post):
            with pytest.raises(Failure, match="deleteFeatures error"):
                resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                )

    def test_raises_on_add_error(self, resource):
        delete_resp = MagicMock()
        delete_resp.json.return_value = {"deleteResults": []}
        add_resp = MagicMock()
        add_resp.json.return_value = {"error": {"code": 500, "message": "Add failed"}}

        def routing_post(url, data=None, **kwargs):
            if url.endswith("/token"):
                return self._token_resp()
            elif url.endswith("/deleteFeatures"):
                return delete_resp
            return add_resp

        df = pd.DataFrame([{"sensor_id": "A"}])

        with patch("orchestrator.resources.arcgis.requests.post", side_effect=routing_post):
            with pytest.raises(Failure, match="addFeatures error"):
                resource.replace_features(
                    layer_url="https://example.com/FeatureServer/0",
                    df=df,
                )
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest orchestrator/tests/resources/test_arcgis.py::TestReplaceFeatures -v
```

Expected: 5 FAILED with `AttributeError: 'ArcGISResource' object has no attribute 'replace_features'`

- [ ] **Step 3: Implement `replace_features`**

In `orchestrator/resources/arcgis.py`, insert this method after `upsert_features` and before `_serialize`:

```python
    def replace_features(
        self,
        layer_url: str,
        df: pd.DataFrame,
        geometry_fields: tuple[str, str] | None = None,
    ) -> dict[str, int]:
        """Delete all existing features then add all rows from df.

        geometry_fields: if provided, (lat_col, lon_col) — attaches point geometry.
        Returns {"deleted": n, "added": n}.
        """
        token = self._get_token()

        del_resp = requests.post(
            f"{layer_url}/deleteFeatures",
            data={"where": "1=1", "f": "json", "token": token},
            timeout=_TIMEOUT,
        )
        del_resp.raise_for_status()
        del_body = del_resp.json()
        if "error" in del_body:
            raise Failure(f"ArcGIS deleteFeatures error: {del_body['error']}")
        deleted = len(del_body.get("deleteResults", []))

        features: list[dict[str, Any]] = []
        for _, row in df.iterrows():
            attrs = {col: _serialize(row[col]) for col in df.columns}
            feature: dict[str, Any] = {"attributes": attrs}
            if geometry_fields:
                lat_col, lon_col = geometry_fields
                feature["geometry"] = {
                    "x": _serialize(row[lon_col]),
                    "y": _serialize(row[lat_col]),
                    "spatialReference": {"wkid": 4326},
                }
            features.append(feature)

        add_resp = requests.post(
            f"{layer_url}/addFeatures",
            data={"features": json.dumps(features), "f": "json", "token": token},
            timeout=_TIMEOUT,
        )
        add_resp.raise_for_status()
        add_body = add_resp.json()
        if "error" in add_body:
            raise Failure(f"ArcGIS addFeatures error: {add_body['error']}")

        added = sum(1 for r in add_body.get("addResults", []) if r.get("success"))
        add_fail = len(add_body.get("addResults", [])) - added
        if add_fail:
            logger.warning(f"ArcGIS addFeatures: {add_fail} failures")

        logger.info(f"ArcGIS replace: {deleted} deleted, {added} added")
        return {"deleted": deleted, "added": added}
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest orchestrator/tests/resources/test_arcgis.py -v
```

Expected: All tests PASSED (existing 8 + new 5 = 13 total).

- [ ] **Step 5: Commit**

```bash
git add orchestrator/resources/arcgis.py orchestrator/tests/resources/test_arcgis.py
git commit -m "feat: add ArcGISResource.replace_features (delete-all then add-all)"
```

---

## Task 3: Update `agol_outdoor_heat_sync` asset (TDD)

**Files:**
- Modify: `orchestrator/assets/outdoor_heat.py`
- Create: `orchestrator/tests/assets/test_outdoor_heat.py`

- [ ] **Step 1: Write failing tests**

Create `orchestrator/tests/assets/test_outdoor_heat.py`:

```python
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
    assert df.loc[df["sensor_id"] == "S2", "radiation_shield"].iloc[0] is False


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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest orchestrator/tests/assets/test_outdoor_heat.py -v
```

Expected: `test_agol_outdoor_heat_sync_*` tests FAIL because the asset still uses `upsert_features` and reads from staging. `test_outdoor_heat_sensor_config_loads_metadata` may pass or fail depending on current state.

- [ ] **Step 3: Update `agol_outdoor_heat_sync` in `outdoor_heat.py`**

**3a.** Add `AssetKey` to the dagster import line (currently line 12):

```python
from dagster import AssetKey, Config, Failure, Output, ResourceParam, asset, get_dagster_logger
```

**3b.** After the `_AGOL_LAYER_URL` line (currently line 22), add:

```python
_OUTDOOR_EXPORT_QUERY = """
    SELECT sensor_id, datetime_edt, temperature_f, relative_humidity_pct,
           dew_point_f, heat_index_f, sensor_name, lat, lon, deployment, radiation_shield
    FROM final.final_outdoor_heat_combined
    ORDER BY sensor_id, datetime_edt
"""
```

**3c.** Replace the entire `agol_outdoor_heat_sync` function (currently lines 288–318) with:

```python
@asset(
    deps=[AssetKey(["final", "final_outdoor_heat_combined"]), "outdoor_heat_sensor_config"],
    compute_kind="python",
    group_name="exports",
)
def agol_outdoor_heat_sync(
    pg_engine: ResourceParam[PostgreConnResources],
    arcgis: ResourceParam[ArcGISResource],
) -> Output[None]:
    """Full-replace the ArcGIS Online outdoor heat feature layer with current data."""
    if not _AGOL_LAYER_URL:
        raise Failure("ARCGIS_OUTDOOR_HEAT_LAYER_URL env var is not set")

    engine = pg_engine.create_engine()
    with engine.connect() as conn:
        df = pd.read_sql(_OUTDOOR_EXPORT_QUERY, conn)

    df["datetime_edt"] = df["datetime_edt"].astype(str)

    result = arcgis.replace_features(
        layer_url=_AGOL_LAYER_URL,
        df=df,
        geometry_fields=("lat", "lon"),
    )

    return Output(
        value=None,
        metadata={
            "features_deleted": result["deleted"],
            "features_added": result["added"],
            "total_rows_synced": len(df),
        },
    )
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest orchestrator/tests/assets/test_outdoor_heat.py -v
```

Expected: All 4 tests PASSED.

- [ ] **Step 5: Run full test suite to check for regressions**

```bash
uv run pytest orchestrator/tests/ -v
```

Expected: All tests PASSED.

- [ ] **Step 6: Commit**

```bash
git add orchestrator/assets/outdoor_heat.py orchestrator/tests/assets/test_outdoor_heat.py
git commit -m "feat: update agol_outdoor_heat_sync to full-replace from final_outdoor_heat_combined"
```

---

## Task 4: Update job + add schedule

**Files:**
- Modify: `orchestrator/jobs/outdoor_heat_job.py`
- Modify: `orchestrator/schedules/mitos_warehouse.py`

- [ ] **Step 1: Update `outdoor_heat_job.py`**

Replace the full contents of `orchestrator/jobs/outdoor_heat_job.py` with:

```python
from dagster import AssetSelection, define_asset_job

outdoor_heat_job = define_asset_job(
    name="outdoor_heat_job",
    selection=(
        AssetSelection.from_string('key:"outdoor_heat_sensor_config"')
        | AssetSelection.from_string('+key:"staging/stg_outdoor_heat_aligned"+')
        | AssetSelection.from_string('+key:"final/final_outdoor_heat_combined"')
        | AssetSelection.from_string('key:"agol_outdoor_heat_sync"')
    ),
)
```

- [ ] **Step 2: Add `outdoor_heat_schedule` to `mitos_warehouse.py`**

**2a.** Add import after the `indoor_heat_job` import line:

```python
from orchestrator.jobs.outdoor_heat_job import outdoor_heat_job
```

**2b.** Add schedule definition after `indoor_heat_schedule`:

```python
outdoor_heat_schedule = ScheduleDefinition(
    job=outdoor_heat_job,
    cron_schedule="0 0 * * 0",  # Sunday midnight UTC
)
```

**2c.** Append `outdoor_heat_schedule` to the `schedules` list:

```python
schedules = [
    build_schedule_from_dbt_selection(
        [mitos_dbt_assets],
        job_name="materialize_dbt_models",
        cron_schedule="0 0 * * *",
        dbt_select="fqn:*",
    ),
    ScheduleDefinition(job=business_asset_job, cron_schedule="0 0 1 * *"),
    website_content_health_schedule,
    indoor_heat_schedule,
    outdoor_heat_schedule,
]
```

- [ ] **Step 3: Verify Dagster definitions load**

```bash
uv run dagster definitions validate -m orchestrator
```

Expected: `Definitions validated successfully` (or no errors if the command differs slightly — check with `uv run dagster --help` if needed).

- [ ] **Step 4: Run full test suite one final time**

```bash
uv run pytest orchestrator/tests/ -v
```

Expected: All tests PASSED.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/jobs/outdoor_heat_job.py orchestrator/schedules/mitos_warehouse.py
git commit -m "feat: add outdoor_heat_job with final_outdoor_heat_combined step and Sunday midnight schedule"
```
