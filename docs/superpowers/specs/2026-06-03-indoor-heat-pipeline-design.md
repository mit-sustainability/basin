# Indoor Heat Pipeline — Enrichment & Calibration Design

**Date:** 2026-06-03  
**Branch:** indoor-heat-pipeline  
**Status:** Approved

## Context

The existing pipeline ingests HOBO sensor Excel files from Dropbox and stores raw readings in Postgres. It deduplicates into `staging.stg_indoor_heat` but does not handle column name variants across sensor configs, unit differences (°F vs °C), heat index, time alignment, or cross-sensor calibration.

## Goals

1. Robust column normalization — handle all known HOBO export column name variants
2. Multi-format ingestion — `.xlsx`, `.xls`, and `.csv` files
3. Unit normalization — detect °F inputs, convert; staging stores °F throughout
4. Heat index — NOAA/Rothfusz formula applied in staging
5. Time alignment — 20-min binned final staging table for analysis and dbt
6. Calibration report — cross-sensor precision + outlier detection, persisted to Postgres

## Asset DAG

```
raw_indoor_heat_sensor
  → stg_indoor_heat              (normalize to °F, add heat_index_f)
      → stg_indoor_heat_aligned  (20-min bins, avg per sensor)
          → indoor_heat_calibration   (cross-sensor stats → Postgres, manual only)
```

The existing `indoor_heat_daily_summary` dbt model is updated to read from `staging.indoor_heat_aligned` instead of `staging.stg_indoor_heat`.

## Changes to Existing Code

### `DropboxResource`

Add `list_sensor_files(folder_path) -> list[tuple[str, str]]` — lists `.xlsx`, `.xls`, and `.csv` files. Existing `list_excel_files` is kept unchanged to avoid breaking other pipelines.

### `_read_sensor_file` (renamed from `_read_sensor_excel`)

- Dispatches on file extension: `pd.read_excel` for `.xlsx`/`.xls`, `pd.read_csv` for `.csv`
- Applies expanded column map covering all known HOBO export variants:
  - Direct renames: `'Date-Time (EDT)'`, `'Date-Time (EST)'`, `'Date-Time (EDT/EST)'`, `'Temperature , °C'`, `'temp , °C'`, `'1 , °C'`, `'RH , %'`, `'rh , %'`, `'1 , %'`, `'Dew Point , °C'`
  - Fahrenheit renames: `'Temperature , °F'`, `'Temperature  , °F'`, `'Dew Point , °F'`, `'Dew Point  , °F'`
- Detects °F columns, converts to °C via `(F - 32) * 5/9`, drops °F intermediates
- Raw always stores °C (canonical SI units for auditability)

### `raw_indoor_heat_sensor`

- Switches from `list_excel_files` to `list_sensor_files`
- No schema change to the raw table

### `stg_indoor_heat`

Schema change (breaking): after dedup, converts °C to °F and adds heat index.

Columns out:
- `sensor_id`, `location`, `datetime_edt`, `source_file`, `last_update`
- `temperature_f` — converted from `temperature_c`
- `relative_humidity_pct` — unchanged
- `dew_point_f` — converted from `dew_point_c`
- `heat_index_f` — NOAA/Rothfusz formula (see below)

`temperature_c` and `dew_point_c` are dropped from the staging output; °F is the canonical unit forward of staging.

**Heat index formula** — vectorized NOAA/Rothfusz:
- If simple estimate `< 80°F`: use `0.5 * (T + 61 + (T - 68) * 1.2 + RH * 0.094)` averaged with T
- Otherwise: full Rothfusz polynomial with low-RH and high-RH adjustments

Existing tests updated to reflect the new column set and °F values.

## New Assets

### `stg_indoor_heat_aligned`

- **Reads:** `staging.stg_indoor_heat`
- **Transform:** round `datetime_edt` to nearest 20-min bin; group by `(sensor_id, location, datetime_bin)`; mean of `temperature_f`, `relative_humidity_pct`, `dew_point_f`, `heat_index_f`
- **Writes:** `staging.indoor_heat_aligned` via `postgres_replace`
- **Added to:** `indoor_heat_job` selection (runs after `stg_indoor_heat`)
- **Metadata:** row count, sensor count, date range

### `indoor_heat_calibration`

- **Reads:** `staging.indoor_heat_aligned`
- **Not part of scheduled job** — lives in its own `indoor_heat_calibration_job` for manual runs
- **Logic** (from `calib_sensor_v2_internal.py`):
  1. Per-timestamp: ensemble mean + std across all sensors for each variable
  2. Per-sensor: compute mean bias vs ensemble mean, std of residuals
  3. Flag outliers: `|bias| > 2σ` of biases across sensors
  4. Classify severity: `> 3σ` → excluded, `2–3σ` → marginal, else pass
- **Writes two tables:**
  - `staging.indoor_heat_calibration_precision` — per-variable mean/median/max σ across all timestamps
  - `staging.indoor_heat_calibration_sensors` — per-sensor bias, std, n_sigma, outlier flag, severity
- **Metadata:** sensor count, outlier count, run timestamp

## Job Changes

| Job | Selection | Schedule |
|-----|-----------|----------|
| `indoor_heat_job` | `+key:"staging/stg_indoor_heat_aligned"+` | existing schedule |
| `indoor_heat_calibration_job` | `indoor_heat_calibration` | manual only |

## File Changes Summary

| File | Change |
|------|--------|
| `orchestrator/resources/dropbox.py` | Add `list_sensor_files` |
| `orchestrator/assets/indoor_heat.py` | Expand column map, rename reader, update `stg_indoor_heat`, add 2 new assets |
| `orchestrator/jobs/indoor_heat_job.py` | Update selection to include `stg_indoor_heat_aligned` |
| `orchestrator/jobs/indoor_heat_calibration_job.py` | New file |
| `orchestrator/__init__.py` | Register new job |
| `warehouse/models/final/indoor_heat_daily_summary.sql` | Update source ref to `indoor_heat_aligned` |
| `orchestrator/tests/assets/test_indoor_heat.py` | Update existing tests, add new tests |
