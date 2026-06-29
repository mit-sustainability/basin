# Indoor Heat Multi-Phase Export — Design Spec

## Context

The student researcher has organized indoor sensor deployments into multiple distinct phases. The same rooms may have different sensor setups (different sensors, placements, window/blind states) across phases. Regular scheduled pipeline updates are no longer the priority; the goal is to produce one clean JSON export per phase for the frontend dashboard.

## Deliverable

A standalone script `scripts/export_indoor_phase.py` that produces, for each phase:

```
output/phase1/manifest.json
output/phase1/readings_<timestamp>.json
output/phase2/manifest.json
output/phase2/readings_<timestamp>.json
...
```

The frontend (student-led) will use the per-phase manifests to power a phase-switching control. The script has no frontend responsibility beyond producing these files.

## What Is Not Needed

- Postgres / database storage
- Dagster assets or scheduling
- Calibration analysis
- A root index manifest (phases are fixed, 4 total)

## Script Design

**File:** `scripts/export_indoor_phase.py`

**CLI:**
```bash
python scripts/export_indoor_phase.py --phase phase1
```

**Phase configuration dict** (top of script, only thing to edit per deployment):
```python
PHASES = {
    "phase1": {
        "dropbox_folder": "ns:4039652928/.../Phase1/Data",
        "config_path":    "ns:4039652928/.../Phase1/sensor_config.json",
        "output_dir":     "./output/phase1",
        "browser_base":   "/data/phase1",
    },
    "phase2": { ... },
    "phase3": { ... },
    "phase4": { ... },
}
```

**Processing flow** (fully in-memory, no Postgres):

1. Init Dropbox client (existing `DropboxResource`)
2. List + download all sensor files from `dropbox_folder`
3. Parse each file: `_parse_sensor_filename` + `_read_sensor_file`
4. Concat → deduplicate by `(sensor_id, datetime_edt)` → compute heat index → bin to 20-min intervals
5. Load sensor config JSON from `config_path`: `_load_sensor_metadata`
6. JOIN sensor readings with config on `sensor_id` (replicates `final_indoor_heat_combined`)
7. Write `readings_<timestamp>.json` + `manifest.json` via `_write_heat_export`

## Code Reuse

All helper functions imported from `orchestrator.assets.indoor_heat`:
- `_parse_sensor_filename`
- `_read_sensor_file`
- `_calculate_heat_index_f`
- `_f_to_c`
- `_load_sensor_metadata`
- `_write_heat_export`

**One small change to `_write_heat_export`:** add `browser_base: str = "/data"` parameter (default preserves existing Dagster behavior). The script passes the per-phase `browser_base` value so the manifest points to the correct `/data/phaseN/` path.

## Output Format

Unchanged from the existing dashboard format:
- `readings_*.json`: array of sensor reading records with room, floor, timestamp, temperature_f, humidity_pct, heat_index_f, orientation, window_state, blinds_state, sensor_photo, window_photo
- `manifest.json`: `{ "generated_at": "...", "files": { "readings": "/data/phaseN/readings_*.json" } }`

## Handoff to Frontend

The student receives 4 output directories. Each `manifest.json` is a self-contained pointer to its phase's readings file. The frontend phase-switching control fetches `/data/phaseN/manifest.json` based on the selected phase — no coordination file needed from the script side.
