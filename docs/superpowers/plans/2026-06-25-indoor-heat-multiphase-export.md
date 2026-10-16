# Indoor Heat Multi-Phase Export — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a standalone script that exports one phase of indoor heat sensor data (from Dropbox) to a JSON readings file + manifest, without Postgres or Dagster.

**Architecture:** One script (`scripts/export_indoor_phase.py`) that runs the full pipeline in-memory — download sensor files from Dropbox, normalize, join with per-phase config, export. Phase configuration lives in a dict at the top of the script. The only change to existing code is adding a `browser_base` param to `_write_heat_export` so each phase's manifest points to its own `/data/phaseN/` path.

**Tech Stack:** Python, pandas, existing `DropboxResource`, existing helpers from `orchestrator.assets.indoor_heat`.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `orchestrator/assets/indoor_heat.py` | Add `browser_base` param to `_write_heat_export` |
| Modify | `orchestrator/tests/assets/test_indoor_heat.py` | Add test for `browser_base` param |
| Create | `scripts/__init__.py` | Makes `scripts/` importable as a package |
| Create | `scripts/export_indoor_phase.py` | The export script |
| Create | `scripts/test_export_indoor_phase.py` | Test for `run_phase` (co-located with script) |

> **Note on running tests:** `scripts/` lives at the repo root, not inside the `orchestrator` package. Run script tests from the repo root with `PYTHONPATH=.` so Python can find both `scripts` and `orchestrator`.

---

## Task 1: Add `browser_base` param to `_write_heat_export`

**Files:**
- Modify: `orchestrator/assets/indoor_heat.py` (the `_write_heat_export` function, currently line ~426)
- Modify: `orchestrator/tests/assets/test_indoor_heat.py` (add one test at the end of the `_write_heat_export` section)

- [ ] **Step 1: Write the failing test**

Add to the end of the `# ── _write_heat_export` section in `orchestrator/tests/assets/test_indoor_heat.py`:

```python
def test_write_heat_export_uses_custom_browser_base(tmp_path):
    now = datetime(2026, 6, 9, 12, 0, 0)
    _, manifest = _write_heat_export(tmp_path, _make_export_df(), now, browser_base="/data/phase2")
    assert manifest["files"]["readings"] == "/data/phase2/readings_20260609T120000Z.json"
```

- [ ] **Step 2: Run test to verify it fails**

Run from the repo root (`/Users/yucheng/Documents/Projects/basin`):

```bash
uv run pytest orchestrator/tests/assets/test_indoor_heat.py::test_write_heat_export_uses_custom_browser_base -v
```

Expected: `FAILED` — `_write_heat_export() got an unexpected keyword argument 'browser_base'`

- [ ] **Step 3: Add `browser_base` param to `_write_heat_export`**

In `orchestrator/assets/indoor_heat.py`, change the function signature and the `browser_path` line:

```python
def _write_heat_export(output_dir: Path, df: pd.DataFrame, now: datetime, browser_base: str = "/data") -> tuple[Path, dict]:
```

And change:
```python
    browser_path = f"/data/{readings_filename}"
```
to:
```python
    browser_path = f"{browser_base}/{readings_filename}"
```

- [ ] **Step 4: Run all `_write_heat_export` tests to verify no regressions**

```bash
uv run pytest orchestrator/tests/assets/test_indoor_heat.py -k "write_heat_export" -v
```

Expected: all 6 existing tests + 1 new test PASS. The existing `test_write_heat_export_manifest_content` still passes because `browser_base` defaults to `"/data"`.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/assets/indoor_heat.py orchestrator/tests/assets/test_indoor_heat.py
git commit -m "feat: add browser_base param to _write_heat_export for per-phase manifest paths"
```

---

## Task 2: Create `scripts/export_indoor_phase.py`

**Files:**
- Create: `scripts/__init__.py`
- Create: `scripts/export_indoor_phase.py`
- Create: `scripts/test_export_indoor_phase.py`

- [ ] **Step 1: Write the failing test**

Create `scripts/test_export_indoor_phase.py`:

```python
import json
from datetime import datetime
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from scripts.export_indoor_phase import PHASES, run_phase


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


def _make_config_json() -> BytesIO:
    config = {
        "3": {
            "hobo_id": 21777605, "calibration_id": 1, "floor": 3,
            "orientation": "East", "window_state": "Closed 24/7",
            "blinds_state": "Open", "note": None,
            "sensor_photo": None, "window_photo": None,
        }
    }
    return BytesIO(json.dumps(config).encode())


def _mock_dropbox():
    dbx = MagicMock()
    dbx.list_sensor_files.return_value = [
        ("MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx",
         "/phase1/MIT+Camb 3 2026-05-15 14_04_50 EDT.xlsx"),
    ]

    def _fake_download(path):
        if path.endswith(".json"):
            return _make_config_json()
        return _make_sensor_excel()

    dbx.download_file.side_effect = _fake_download
    return dbx


def test_run_phase_writes_readings_and_manifest(tmp_path, monkeypatch):
    # Point phase1 output to tmp_path
    monkeypatch.setitem(PHASES, "phase1", {**PHASES["phase1"], "output_dir": str(tmp_path)})
    run_phase("phase1", _mock_dropbox())
    assert (tmp_path / "manifest.json").exists()
    readings = list(tmp_path.glob("readings_*.json"))
    assert len(readings) == 1


def test_run_phase_manifest_points_to_correct_browser_base(tmp_path, monkeypatch):
    monkeypatch.setitem(PHASES, "phase1", {**PHASES["phase1"], "output_dir": str(tmp_path)})
    run_phase("phase1", _mock_dropbox())
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["files"]["readings"].startswith("/data/phase1/")


def test_run_phase_readings_include_config_fields(tmp_path, monkeypatch):
    monkeypatch.setitem(PHASES, "phase1", {**PHASES["phase1"], "output_dir": str(tmp_path)})
    run_phase("phase1", _mock_dropbox())
    readings = json.loads(list(tmp_path.glob("readings_*.json"))[0].read_text())
    assert readings[0]["orientation"] == "East"
    assert readings[0]["floor"] == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run from the repo root:

```bash
PYTHONPATH=. uv run pytest scripts/test_export_indoor_phase.py -v
```

Expected: `ERROR` — `ModuleNotFoundError: No module named 'scripts.export_indoor_phase'`

- [ ] **Step 3: Create `scripts/__init__.py` and `scripts/export_indoor_phase.py`**

```bash
mkdir -p scripts
touch scripts/__init__.py
```

Then create `scripts/export_indoor_phase.py`:

```python
"""Export one indoor-heat phase to JSON + manifest.

Usage:
    python scripts/export_indoor_phase.py --phase phase1

Fill in PHASES below with the Dropbox paths the student provides for each phase.
"""

import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

from orchestrator.assets.indoor_heat import (
    _calculate_heat_index_f,
    _load_sensor_metadata,
    _parse_sensor_filename,
    _read_sensor_file,
    _write_heat_export,
)
from orchestrator.resources.dropbox import DropboxResource

# ── Fill these in once the student provides the Dropbox folder structure ───────
PHASES: dict[str, dict] = {
    "phase1": {
        "dropbox_folder": "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase1",
        "config_path":    "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase1/sensor_config.json",
        "output_dir":     "./output/phase1",
        "browser_base":   "/data/phase1",
    },
    "phase2": {
        "dropbox_folder": "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase2",
        "config_path":    "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase2/sensor_config.json",
        "output_dir":     "./output/phase2",
        "browser_base":   "/data/phase2",
    },
    "phase3": {
        "dropbox_folder": "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase3",
        "config_path":    "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase3/sensor_config.json",
        "output_dir":     "./output/phase3",
        "browser_base":   "/data/phase3",
    },
    "phase4": {
        "dropbox_folder": "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase4",
        "config_path":    "ns:4039652928/Program Topics/Data/Projects/Indoor campus heat data 2026/Phase4/sensor_config.json",
        "output_dir":     "./output/phase4",
        "browser_base":   "/data/phase4",
    },
}
# ──────────────────────────────────────────────────────────────────────────────


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate, convert to °F, compute heat index, bin to 20-min intervals."""
    df = df.sort_values("last_update", ascending=False).drop_duplicates(
        subset=["sensor_id", "datetime_edt"], keep="first"
    )
    df = df.copy()
    df["temperature_f"] = df["temperature_c"] * 9 / 5 + 32
    df["dew_point_f"] = df["dew_point_c"] * 9 / 5 + 32
    df["heat_index_f"] = _calculate_heat_index_f(df["temperature_f"], df["relative_humidity_pct"])
    df["datetime_bin"] = df["datetime_edt"].dt.round("20min")
    return (
        df.groupby(["sensor_id", "datetime_bin"])
        .agg(
            temperature_f=("temperature_f", "mean"),
            relative_humidity_pct=("relative_humidity_pct", "mean"),
            dew_point_f=("dew_point_f", "mean"),
            heat_index_f=("heat_index_f", "mean"),
        )
        .reset_index()
        .rename(columns={"datetime_bin": "datetime_edt"})
    )


def run_phase(phase: str, dropbox: DropboxResource) -> None:
    cfg = PHASES[phase]

    all_files = dropbox.list_sensor_files(cfg["dropbox_folder"])
    if not all_files:
        raise SystemExit(f"No sensor files found in {cfg['dropbox_folder']!r}")

    frames = []
    for name, path in all_files:
        try:
            meta = _parse_sensor_filename(name)
            meta["source_file"] = name
            df = _read_sensor_file(dropbox.download_file(path), meta)
            frames.append(df)
            print(f"  {name}: {len(df)} rows")
        except Exception as exc:
            print(f"  SKIP {name}: {exc}")

    if not frames:
        raise SystemExit("All files failed to parse — check output above")

    combined = pd.concat(frames, ignore_index=True)
    combined["last_update"] = datetime.now()

    aligned = _normalize(combined)
    config_df = _load_sensor_metadata(dropbox, cfg["config_path"])
    merged = aligned.merge(config_df, on="sensor_id", how="left")

    output_dir = Path(cfg["output_dir"])
    readings_path, manifest = _write_heat_export(
        output_dir, merged, datetime.utcnow(), browser_base=cfg["browser_base"]
    )
    print(f"  wrote {readings_path}")
    print(f"  manifest → {manifest['files']['readings']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Export one indoor-heat phase to JSON.")
    parser.add_argument("--phase", required=True, choices=list(PHASES), help="Phase to export")
    args = parser.parse_args()

    print(f"Exporting {args.phase}...")
    run_phase(args.phase, DropboxResource())
    print("Done.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=. uv run pytest scripts/test_export_indoor_phase.py -v
```

Expected: 3 tests PASS.

- [ ] **Step 5: Run the full test suite to catch any regressions**

```bash
uv run pytest orchestrator/tests/ -v
PYTHONPATH=. uv run pytest scripts/test_export_indoor_phase.py -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add scripts/__init__.py scripts/export_indoor_phase.py scripts/test_export_indoor_phase.py
git commit -m "feat: add export_indoor_phase script for multi-phase indoor heat data export"
```

---

## After Implementation

Update the `PHASES` dict in `scripts/export_indoor_phase.py` once the student confirms the actual Dropbox folder paths and phase names. The script can then be run as:

```bash
uv run python scripts/export_indoor_phase.py --phase phase1
uv run python scripts/export_indoor_phase.py --phase phase2
uv run python scripts/export_indoor_phase.py --phase phase3
uv run python scripts/export_indoor_phase.py --phase phase4
```

Each run produces `output/phaseN/manifest.json` + `output/phaseN/readings_*.json`.
