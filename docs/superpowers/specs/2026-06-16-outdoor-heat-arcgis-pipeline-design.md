# Outdoor Heat Pipeline — ArcGIS Online Export Design

**Date:** 2026-06-16
**Branch:** outdoor-heat-pipeline
**Status:** Approved

## Overview

Adapt the indoor heat pipeline pattern for outdoor sensors, replacing the JSON file export with a push to an ArcGIS Online (AGOL) hosted feature layer. The layer receives 20-minute binned readings joined with sensor metadata (lat/lon/name) so that ArcGIS views and dashboards pick up updates automatically.

## Merge Conflict Resolution

### `orchestrator/constants.py`
Keep HEAD's additions (not present in main):
```python
dropbox_token = os.getenv("DROPBOX_ACCESS_TOKEN")
ARCGIS_CLIENT_ID = os.getenv("ARCGIS_CLIENT_ID", "")
ARCGIS_CLIENT_SECRET = os.getenv("ARCGIS_CLIENT_SECRET", "")
ARCGIS_ORG_URL = os.getenv("ARCGIS_ORG_URL", "https://www.arcgis.com")
```

### `warehouse/models/sources.yml`
- `indoor_heat_sensor_config` columns: use incoming (main) version — the detailed set with `hobo_id`, `calibration_id`, `floor`, `orientation`, `window_state`, `blinds_state`, `note`, `sensor_photo`, `window_photo`.
- Keep HEAD's `outdoor_heat_sensor` and `outdoor_heat_sensor_config` source table entries below.

## Data Flow

```
Dropbox (xlsx/csv)
  └─► outdoor_heat_sensor          [raw, Dagster, postgres_replace]
  └─► outdoor_heat_sensor_config   [raw, Dagster, postgres_replace]
        │
        ▼
  stg_outdoor_heat_aligned         [staging, dbt — dedup, °F, heat index, 20-min bins]
        │
        ▼
  final_outdoor_heat_combined      [final, dbt — join with sensor metadata]
        │
        ▼
  agol_outdoor_heat_sync           [exports, Dagster — full replace to AGOL]
        │
        ▼
  ArcGIS Online hosted feature layer
```

## Components

### 1. `warehouse/models/final/final_outdoor_heat_combined.sql` (new)

Mirrors `final_indoor_heat_combined.sql`. Joins 20-min binned readings with sensor config to attach spatial and descriptive metadata:

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

Add its entry to `warehouse/models/final/schema.yml`.

### 2. `ArcGISResource.replace_features` (new method)

Added to `orchestrator/resources/arcgis.py` alongside the existing `upsert_features`:

```python
def replace_features(
    self,
    layer_url: str,
    df: pd.DataFrame,
    geometry_fields: tuple[str, str] | None = None,  # (lat_col, lon_col)
) -> dict[str, int]:
```

Steps:
1. Get OAuth2 token
2. `deleteFeatures` with `where=1=1` — removes all existing features
3. Build feature list from df; if `geometry_fields` is set, attach `{"x": lon, "y": lat, "spatialReference": {"wkid": 4326}}` geometry per feature
4. `addFeatures` with full list
5. Return `{"deleted": n, "added": n}`

### 3. `agol_outdoor_heat_sync` asset (updated)

In `orchestrator/assets/outdoor_heat.py`:
- **Reads from:** `final.final_outdoor_heat_combined` (was `staging.stg_outdoor_heat_aligned`)
- **Calls:** `arcgis.replace_features(layer_url=_AGOL_LAYER_URL, df=df, geometry_fields=("lat", "lon"))`
- **deps:** `[AssetKey(["final", "final_outdoor_heat_combined"]), "outdoor_heat_sensor_config"]`
- **group_name:** `"exports"` (was `"staging"`)
- Env var guard on `ARCGIS_OUTDOOR_HEAT_LAYER_URL` stays as-is

### 4. `orchestrator/jobs/outdoor_heat_job.py` (new)

```python
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

`outdoor_heat_sensor` (raw Dropbox ingest) is kept outside the job — run separately, same pattern as indoor.

### 5. Schedule (updated `mitos_warehouse.py`)

```python
outdoor_heat_schedule = ScheduleDefinition(
    job=outdoor_heat_job,
    cron_schedule="0 0 * * 0",  # Sunday midnight UTC
)
```

Added to the `schedules` list. Runs on the same cadence as `indoor_heat_schedule`.

## Environment Variables

| Variable | Description |
|---|---|
| `ARCGIS_CLIENT_ID` | OAuth2 App ID for AGOL |
| `ARCGIS_CLIENT_SECRET` | OAuth2 App secret |
| `ARCGIS_ORG_URL` | Org URL (default: `https://www.arcgis.com`) |
| `ARCGIS_OUTDOOR_HEAT_LAYER_URL` | REST URL of the target hosted feature layer |
| `DROPBOX_ACCESS_TOKEN` | Dropbox token for sensor file downloads |

## What Is Not Changing

- `outdoor_heat_daily_summary.sql` — kept as-is (useful for other consumers)
- `outdoor_heat_sensor` raw ingest asset — unchanged, run separately
- `indoor_heat_export` and its JSON export pattern — unchanged
- `ArcGISResource.upsert_features` — kept for future use by other assets
