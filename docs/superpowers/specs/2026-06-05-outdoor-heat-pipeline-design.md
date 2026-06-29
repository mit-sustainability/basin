# Outdoor Heat Sensor Pipeline + ArcGIS Online Sync — Design Spec

**Date:** 2026-06-05
**Author:** Yu Cheng

---

## Context

The project already has a working indoor heat sensor pipeline that ingests HOBO sensor files from Dropbox, normalizes them into PostgreSQL, runs a dbt daily aggregation, and includes a calibration QA asset. This spec designs a parallel outdoor heat pipeline that reuses that entire stack nearly verbatim, with one key addition: a Dagster asset that syncs the aligned 20-minute time series to an existing ArcGIS Online hosted feature layer so it can be picked up by an existing dashboard.

The outdoor sensor files are the same HOBO format (.xlsx/.csv), just in a different Dropbox folder.

---

## Architecture & Data Flow

```
Dropbox (outdoor HOBO files)
  │
  ├─▶ [outdoor_heat_sensor]         → raw.outdoor_heat_sensor
  └─▶ [outdoor_heat_sensor_config]  → raw.outdoor_heat_sensor_config
              │
              ▼
      [stg_outdoor_heat_aligned]    → staging.stg_outdoor_heat_aligned
              │
     ┌────────┴────────┐
     ▼                 ▼
[agol_outdoor_     dbt model:
  heat_sync]       outdoor_heat_daily_summary
     │                 │
     ▼                 ▼
ArcGIS Online     final.outdoor_heat_daily_summary
hosted feature
layer (upsert)
```

---

## Components

### 1. Dagster Asset Module — `orchestrator/assets/outdoor_heat.py`

Mirror `indoor_heat.py` with these changes:
- `OutdoorHeatConfig(Config)` — different `dropbox_folder` default pointing to the outdoor data path
- Asset keys use `outdoor_heat_sensor` / `outdoor_heat_sensor_config` / `stg_outdoor_heat_aligned`
- Asset group: `outdoor_heat`
- No calibration asset for now (can be added later if needed)

Add one new asset: `agol_outdoor_heat_sync` (see Section 3).

### 2. ArcGIS Resource — `orchestrator/resources/arcgis.py`

New `ConfigurableResource` following the same shape as `datahub.py`:

```python
class ArcGISResource(ConfigurableResource):
    client_id: str        # from ARCGIS_CLIENT_ID env var
    client_secret: str    # from ARCGIS_CLIENT_SECRET env var
    org_url: str          # e.g. https://www.arcgis.com

    def get_token(self) -> str:
        # POST to {org_url}/sharing/rest/oauth2/token
        # client_credentials grant, returns access token

    def query_features(self, layer_url: str, out_fields: list[str]) -> list[dict]:
        # GET {layer_url}/query?where=1=1&outFields=...&f=json
        # Returns list of {attributes: {...}, OBJECTID: int} dicts

    def upsert_features(
        self,
        layer_url: str,
        df: pd.DataFrame,
        key_fields: list[str],
    ) -> None:
        # 1. query_features to get all existing (key_fields + OBJECTID)
        # 2. Build lookup: (sensor_id, datetime_edt) → OBJECTID
        # 3. Split df rows into adds (no match) and updates (matched, inject OBJECTID)
        # 4. POST {layer_url}/applyEdits with adds=[] and updates=[]
```

Authentication: OAuth2 `client_credentials` grant (App ID + Secret). Token is fetched per-materialization (short-lived, no caching needed for batch sync).

### 3. ArcGIS Sync Asset — `agol_outdoor_heat_sync`

In `outdoor_heat.py`, one additional asset at the bottom:

```python
@asset(deps=["stg_outdoor_heat_aligned"], group_name="outdoor_heat")
def agol_outdoor_heat_sync(context, pg_engine: PostgreConnResources, arcgis: ArcGISResource):
    # 1. Read staging.stg_outdoor_heat_aligned from Postgres via pg_engine
    # 2. Call arcgis.upsert_features(layer_url, df, key_fields=["sensor_id", "datetime_edt"])
    # 3. Log adds/updates counts
```

Layer URL is loaded from `ARCGIS_OUTDOOR_HEAT_LAYER_URL` env var, passed through constants.

### 4. dbt Model — `warehouse/models/final/outdoor_heat_daily_summary.sql`

Mirror `indoor_heat_daily_summary.sql`:
- JOIN `staging.stg_outdoor_heat_aligned` with `raw.outdoor_heat_sensor_config`
- Group by `(sensor_id, date, sensor_name, lat, lon, deployment, radiation_shield)`
- Aggregate: `min/avg/max` for `temperature_f`, `relative_humidity_pct`, `dew_point_f`, `heat_index_f`; count 20-min readings

Add schema doc entry to `warehouse/models/final/schema.yml`.

### 5. Job — `orchestrator/jobs/outdoor_heat_job.py`

Mirror `indoor_heat_job.py`:
- Selection: `outdoor_heat_sensor`, `outdoor_heat_sensor_config`, `stg_outdoor_heat_aligned`, `agol_outdoor_heat_sync`
- `agol_outdoor_heat_sync` runs last (after stg_aligned)

---

## Modified Files

| File | Change |
|------|--------|
| `orchestrator/__init__.py` | Import outdoor_heat module, register assets, arcgis resource, outdoor_heat_job |
| `orchestrator/constants.py` | Add `ARCGIS_CLIENT_ID`, `ARCGIS_CLIENT_SECRET`, `ARCGIS_ORG_URL`, `ARCGIS_OUTDOOR_HEAT_LAYER_URL` |
| `warehouse/models/final/schema.yml` | Add `outdoor_heat_daily_summary` doc block |
| `.envrc` | Add four new ARCGIS_* env var entries |

No changes to `pyproject.toml` — `requests` is already a dependency.

---

## Environment Variables

```bash
ARCGIS_CLIENT_ID=...            # Registered app client ID
ARCGIS_CLIENT_SECRET=...        # Registered app client secret
ARCGIS_ORG_URL=https://www.arcgis.com
ARCGIS_OUTDOOR_HEAT_LAYER_URL=https://services.arcgis.com/.../FeatureServer/0
```

---

## Testing

Since the outdoor pipeline's data format is identical to indoor, there is no value in mirroring the parsing/normalization/heat-index unit tests. Testing focuses on the new ArcGIS integration only.

**`orchestrator/tests/resources/test_arcgis.py`:**
- Token exchange — mock `requests.post`, assert correct grant_type and credentials sent
- Upsert split logic — given existing features (A, B) and incoming (B, C): assert adds=[C], updates=[B with OBJECTID]
- `applyEdits` payload shape — assert correct JSON structure sent to layer URL
- Error handling — non-200 response from AGOL raises a clear exception with context

**Manual end-to-end verification:**
1. Materialize `outdoor_heat_sensor` — check row count in `raw.outdoor_heat_sensor`
2. Materialize `stg_outdoor_heat_aligned` — spot-check 20-min bin timestamps
3. Materialize `agol_outdoor_heat_sync` — confirm Dagster logs show adds/updates counts, verify in ArcGIS Online dashboard
4. Run `dbt run -s outdoor_heat_daily_summary` — confirm `final.outdoor_heat_daily_summary` populated

---

## Key Reused Utilities

| Utility | Path | Used for |
|---------|------|---------|
| `DropboxResource` | `orchestrator/resources/dropbox.py` | File listing + download (unchanged) |
| `PostgreSQLPandasIOManager` | `orchestrator/resources/postgres_io_manager.py` | Raw/staging writes (unchanged) |
| `PostgreConnResources` | `orchestrator/resources/postgres_io_manager.py` | Reading staging table for AGOL sync |
| Pandera schema & `_compute_heat_index` | `orchestrator/assets/indoor_heat.py` | Duplicate into `outdoor_heat.py` (same logic, avoid cross-module import) |
| DataHub factory pattern | `orchestrator/assets/utils.py` | Reference for AGOL resource/asset shape |
