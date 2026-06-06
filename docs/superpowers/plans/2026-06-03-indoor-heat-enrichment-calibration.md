# Indoor Heat Pipeline — Enrichment & Calibration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand the indoor heat pipeline with robust column normalization, CSV support, °F/heat-index staging, 20-min aligned final table, and a one-off cross-sensor calibration asset.

**Architecture:** Approach A — expand `_read_sensor_file` in the existing raw asset for column variants and file formats; modify `stg_indoor_heat` to normalize to °F with heat index; add `stg_indoor_heat_aligned` for 20-min binning; add `indoor_heat_calibration` as a manual-only asset writing to two Postgres tables.

**Tech Stack:** Python, Pandas, NumPy, Dagster, pandera, SQLAlchemy, openpyxl, pytest

---

## File Map

| File | Change |
|------|--------|
| `orchestrator/resources/dropbox.py` | Add `list_sensor_files` (xlsx/xls/csv) |
| `orchestrator/assets/indoor_heat.py` | Expand column map; rename reader; add heat index; update stg; add 2 new assets |
| `orchestrator/jobs/indoor_heat_job.py` | Update selection to include `stg_indoor_heat_aligned` |
| `orchestrator/jobs/indoor_heat_calibration_job.py` | **New** — manual calibration job |
| `orchestrator/__init__.py` | Register calibration job |
| `warehouse/models/final/indoor_heat_daily_summary.sql` | Update source to `stg_indoor_heat_aligned` and column names to °F |
| `warehouse/models/sources.yml` | Update `stg_indoor_heat` columns to °F; add `stg_indoor_heat_aligned` entry |
| `orchestrator/tests/assets/test_indoor_heat.py` | Update existing tests; add new tests |

---

## Task 1: Add `list_sensor_files` to DropboxResource

**Files:**
- Modify: `orchestrator/resources/dropbox.py`
- Test: `orchestrator/tests/assets/test_indoor_heat.py`

- [ ] **Step 1.1: Write the failing tests**

Add to `orchestrator/tests/assets/test_indoor_heat.py`:

```python
from unittest.mock import MagicMock, patch
from orchestrator.resources.dropbox import DropboxResource


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

    assert len(files) == 2
```

- [ ] **Step 1.2: Run tests to verify they fail**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py::test_list_sensor_files_includes_xlsx_xls_and_csv orchestrator/tests/assets/test_indoor_heat.py::test_list_sensor_files_paginates -v
```

Expected: FAIL with `AttributeError: 'DropboxResource' object has no attribute 'list_sensor_files'`

- [ ] **Step 1.3: Implement `list_sensor_files`**

In `orchestrator/resources/dropbox.py`, add after the `list_excel_files` method:

```python
def list_sensor_files(self, folder_path: str) -> list[tuple[str, str]]:
    """Return (filename, path) for all .xlsx/.xls/.csv files in folder_path."""
    dbx = dropbox.Dropbox(oauth2_access_token=self.access_token)
    result = dbx.files_list_folder(folder_path)
    entries = list(result.entries)
    while result.has_more:
        result = dbx.files_list_folder_continue(result.cursor)
        entries.extend(result.entries)

    files = [
        (e.name, e.path_lower)
        for e in entries
        if hasattr(e, "name") and e.name.lower().endswith((".xlsx", ".xls", ".csv"))
    ]
    logger.info(f"Found {len(files)} sensor files in {folder_path}")
    return files
```

- [ ] **Step 1.4: Run tests to verify they pass**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py::test_list_sensor_files_includes_xlsx_xls_and_csv orchestrator/tests/assets/test_indoor_heat.py::test_list_sensor_files_paginates -v
```

Expected: 2 passed

- [ ] **Step 1.5: Commit**

```bash
git add orchestrator/resources/dropbox.py orchestrator/tests/assets/test_indoor_heat.py
git commit -m "feat: add list_sensor_files to DropboxResource (xlsx/xls/csv)"
```

---

## Task 2: Expand column normalization — `_read_sensor_file` with CSV support and °F→°C

**Files:**
- Modify: `orchestrator/assets/indoor_heat.py`
- Modify: `orchestrator/tests/assets/test_indoor_heat.py`

- [ ] **Step 2.1: Write the failing tests**

Add to `orchestrator/tests/assets/test_indoor_heat.py`:

```python
import io
import numpy as np
from orchestrator.assets.indoor_heat import _read_sensor_file


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
        "Temperature , °F": [71.528, 71.24],  # 21.96°C and 21.80°C
        "RH , %": [41.92, 42.12],
        "Dew Point , °F": [47.21, 47.08],
    })
    buf = BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    return buf


def _make_sensor_csv() -> BytesIO:
    content = (
        "Date-Time (EST),temp , °C,rh , %\n"
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
```

Also update the existing test that imports `_read_sensor_excel`:

```python
# Change this import at the top of the test file:
from orchestrator.assets.indoor_heat import (
    IndoorHeatConfig,
    _parse_sensor_filename,
    _read_sensor_file,          # renamed from _read_sensor_excel
    raw_indoor_heat_sensor,
)

# Update the test name and body:
def test_read_sensor_file_standardizes_columns():  # was: test_read_sensor_excel_standardizes_columns
    meta = {"location": "MIT+Camb", "sensor_id": "3", "source_file": "test.xlsx"}
    result = _read_sensor_file(_make_sensor_excel(), meta)  # _make_sensor_excel still works (°C, no-space)
    assert list(result.columns) == [
        "row_num", "datetime_edt", "temperature_c",
        "relative_humidity_pct", "dew_point_c",
        "sensor_id", "location", "source_file",
    ]
    assert len(result) == 2
    assert result["sensor_id"].iloc[0] == "3"
    assert result["location"].iloc[0] == "MIT+Camb"
```

- [ ] **Step 2.2: Run tests to verify they fail**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py -k "read_sensor_file" -v
```

Expected: FAIL with `ImportError: cannot import name '_read_sensor_file'`

- [ ] **Step 2.3: Replace column map and reader in `orchestrator/assets/indoor_heat.py`**

Remove the old `_COLUMN_MAP` constant and `_read_sensor_excel` function. Replace with:

```python
import numpy as np  # add to imports at top of file

_DIRECT_RENAMES = {
    "#": "row_num",
    "Date-Time (EDT)": "datetime_edt",
    "Date-Time (EST)": "datetime_edt",
    "Date-Time (EDT/EST)": "datetime_edt",
    "Temperature , °C": "temperature_c",
    "Temperature, °C": "temperature_c",
    "temp , °C": "temperature_c",
    "1 , °C": "temperature_c",
    "RH , %": "relative_humidity_pct",
    "RH, %": "relative_humidity_pct",
    "rh , %": "relative_humidity_pct",
    "1 , %": "relative_humidity_pct",
    "Dew Point , °C": "dew_point_c",
    "Dew Point, °C": "dew_point_c",
}

_FAHRENHEIT_RENAMES = {
    "Temperature , °F": "temp_f_raw",
    "Temperature  , °F": "temp_f_raw",
    "Temperature, °F": "temp_f_raw",
    "Dew Point , °F": "dew_point_f_raw",
    "Dew Point  , °F": "dew_point_f_raw",
    "Dew Point, °F": "dew_point_f_raw",
}


def _f_to_c(series: pd.Series) -> pd.Series:
    return (series - 32) * 5 / 9


def _read_sensor_file(file_bytes: BytesIO, meta: dict) -> pd.DataFrame:
    """Load a sensor file (.xlsx/.xls/.csv), normalize all column variants to °C."""
    ext = meta["source_file"].rsplit(".", 1)[-1].lower()
    df = pd.read_csv(file_bytes) if ext == "csv" else pd.read_excel(file_bytes, engine="openpyxl")

    df = df.rename(columns=_DIRECT_RENAMES | _FAHRENHEIT_RENAMES)

    for f_col, c_col in [("temp_f_raw", "temperature_c"), ("dew_point_f_raw", "dew_point_c")]:
        if f_col in df.columns:
            df[c_col] = _f_to_c(df[f_col])
            df = df.drop(columns=f_col)

    if "row_num" not in df.columns:
        df["row_num"] = range(len(df))

    if "dew_point_c" not in df.columns:
        df["dew_point_c"] = float("nan")

    required = {"datetime_edt", "temperature_c", "relative_humidity_pct"}
    missing = required - set(df.columns)
    if missing:
        raise Failure(f"{meta['source_file']}: missing columns {missing} after normalization")

    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])
    df["sensor_id"] = meta["sensor_id"]
    df["location"] = meta["location"]
    df["source_file"] = meta["source_file"]

    return df[[
        "row_num", "datetime_edt", "temperature_c",
        "relative_humidity_pct", "dew_point_c",
        "sensor_id", "location", "source_file",
    ]]
```

Also update `IndoorHeatSensorRawSchema` to make `dew_point_c` nullable:

```python
dew_point_c: Series[float] = pa.Field(description="Dew Point in Celsius", nullable=True)
```

- [ ] **Step 2.4: Update `raw_indoor_heat_sensor` call sites**

In `raw_indoor_heat_sensor`, make two changes:

1. Change `dropbox.list_excel_files(config.dropbox_folder)` → `dropbox.list_sensor_files(config.dropbox_folder)`
2. Change `df = _read_sensor_excel(file_bytes, meta)` → `df = _read_sensor_file(file_bytes, meta)`

- [ ] **Step 2.5: Run all tests**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py -v
```

Expected: all tests pass (including the renamed `test_read_sensor_file_standardizes_columns`)

- [ ] **Step 2.6: Commit**

```bash
git add orchestrator/assets/indoor_heat.py orchestrator/tests/assets/test_indoor_heat.py
git commit -m "feat: expand column normalization, add CSV support, handle °F inputs in _read_sensor_file"
```

---

## Task 3: Add `_calculate_heat_index_f` and update `stg_indoor_heat` to °F

**Files:**
- Modify: `orchestrator/assets/indoor_heat.py`
- Modify: `orchestrator/tests/assets/test_indoor_heat.py`

- [ ] **Step 3.1: Write the failing tests**

Add to `orchestrator/tests/assets/test_indoor_heat.py`:

```python
from orchestrator.assets.indoor_heat import _calculate_heat_index_f, stg_indoor_heat


def test_calculate_heat_index_f_uses_simple_estimate_below_80():
    # At 70°F / 50% RH: hi_simple = 0.5*(70+61+(70-68)*1.2+50*0.094) = 69.05; avg with 70 = 69.525
    result = _calculate_heat_index_f(pd.Series([70.0]), pd.Series([50.0]))
    assert result.iloc[0] == pytest.approx(69.525, abs=0.1)


def test_calculate_heat_index_f_uses_rothfusz_above_80():
    # At 90°F / 50% RH: NOAA reference ≈ 95°F
    result = _calculate_heat_index_f(pd.Series([90.0]), pd.Series([50.0]))
    assert result.iloc[0] == pytest.approx(95.0, abs=1.0)


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
```

- [ ] **Step 3.2: Run tests to verify they fail**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py -k "heat_index or stg_indoor_heat" -v
```

Expected: FAIL with `ImportError: cannot import name '_calculate_heat_index_f'`

- [ ] **Step 3.3: Add `_calculate_heat_index_f` to `orchestrator/assets/indoor_heat.py`**

Add this function after `_f_to_c`:

```python
def _calculate_heat_index_f(temp_f: pd.Series, rh: pd.Series) -> pd.Series:
    """NOAA/Rothfusz heat index. Inputs and output in °F."""
    hi_simple = 0.5 * (temp_f + 61.0 + ((temp_f - 68.0) * 1.2) + (rh * 0.094))
    hi_simple = (hi_simple + temp_f) / 2

    c1, c2, c3, c4 = -42.379, 2.04901523, 10.14333127, -0.22475541
    c5, c6, c7, c8, c9 = -0.00683783, -0.05481717, 0.00122874, 0.00085282, -0.00000199
    hi_full = (c1 + c2 * temp_f + c3 * rh + c4 * temp_f * rh
               + c5 * temp_f**2 + c6 * rh**2
               + c7 * temp_f**2 * rh + c8 * temp_f * rh**2
               + c9 * temp_f**2 * rh**2)

    adj_low = ((13 - rh) / 4) * np.sqrt(np.maximum(0, (17 - np.abs(temp_f - 95)) / 17))
    hi_full = np.where((rh < 13) & (temp_f >= 80) & (temp_f <= 112), hi_full - adj_low, hi_full)

    adj_high = ((rh - 85) / 10) * ((87 - temp_f) / 5)
    hi_full = np.where((rh > 85) & (temp_f >= 80) & (temp_f <= 87), hi_full + adj_high, hi_full)

    return pd.Series(np.where(hi_simple < 80, hi_simple, hi_full), index=temp_f.index)
```

- [ ] **Step 3.4: Update `stg_indoor_heat` in `orchestrator/assets/indoor_heat.py`**

Replace the body of `stg_indoor_heat` with:

```python
@asset(
    deps=[raw_indoor_heat_sensor],
    io_manager_key="postgres_replace",
    compute_kind="python",
    key_prefix="staging",
    group_name="staging",
)
def stg_indoor_heat(pg_engine: ResourceParam[PostgreConnResources]) -> Output[pd.DataFrame]:
    """Deduplicate raw readings and normalize to °F with heat index."""
    engine = pg_engine.create_engine()
    df = pd.read_sql_query("SELECT * FROM raw.indoor_heat_sensor", engine)

    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])
    df["temperature_c"] = df["temperature_c"].astype(float)
    df["relative_humidity_pct"] = df["relative_humidity_pct"].astype(float)
    df["dew_point_c"] = df["dew_point_c"].astype(float)

    before = len(df)
    df = df.sort_values("last_update", ascending=False).drop_duplicates(
        subset=["sensor_id", "datetime_edt"], keep="first"
    )
    logger.info(f"Deduplicated {before} -> {len(df)} rows")

    df["temperature_f"] = df["temperature_c"] * 9 / 5 + 32
    df["dew_point_f"] = df["dew_point_c"] * 9 / 5 + 32
    df["heat_index_f"] = _calculate_heat_index_f(df["temperature_f"], df["relative_humidity_pct"])

    out_cols = [
        "sensor_id", "location", "datetime_edt",
        "temperature_f", "relative_humidity_pct", "dew_point_f", "heat_index_f",
        "source_file", "last_update",
    ]
    return Output(
        value=df[out_cols],
        metadata={"total_rows": len(df), "unique_sensors": df["sensor_id"].nunique()},
    )
```

- [ ] **Step 3.5: Run all tests**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py -v
```

Expected: all tests pass

- [ ] **Step 3.6: Commit**

```bash
git add orchestrator/assets/indoor_heat.py orchestrator/tests/assets/test_indoor_heat.py
git commit -m "feat: add heat index calculation, normalize stg_indoor_heat to Fahrenheit"
```

---

## Task 4: Add `stg_indoor_heat_aligned` asset

**Files:**
- Modify: `orchestrator/assets/indoor_heat.py`
- Modify: `orchestrator/tests/assets/test_indoor_heat.py`

- [ ] **Step 4.1: Write the failing tests**

Add to `orchestrator/tests/assets/test_indoor_heat.py`:

```python
from orchestrator.assets.indoor_heat import stg_indoor_heat_aligned


def _make_stg_df() -> pd.DataFrame:
    # Sensor A: two readings in same 20-min bin (12:00 and 12:05 both → 12:00)
    # Sensor B: one reading per bin
    return pd.DataFrame({
        "sensor_id": ["A", "A", "B", "B"],
        "location": ["MIT"] * 4,
        "datetime_edt": pd.to_datetime([
            "2026-05-15 12:00:00",
            "2026-05-15 12:05:00",
            "2026-05-15 12:00:00",
            "2026-05-15 12:20:00",
        ]),
        "temperature_f": [71.0, 73.0, 70.0, 72.0],
        "relative_humidity_pct": [40.0, 42.0, 41.0, 43.0],
        "dew_point_f": [47.0, 49.0, 46.0, 48.0],
        "heat_index_f": [71.0, 73.0, 70.0, 72.0],
        "source_file": ["a.xlsx"] * 4,
        "last_update": pd.to_datetime(["2026-05-15"] * 4),
    })


def test_stg_indoor_heat_aligned_collapses_readings_into_20min_bins():
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=_make_stg_df()):
        result = stg_indoor_heat_aligned(pg_engine=MagicMock())
    df = result.value
    # Sensor A: 12:00 and 12:05 collapse to one bin (12:00)
    # Sensor B: 12:00 and 12:20 are separate bins
    # Total: 3 rows
    assert len(df) == 3


def test_stg_indoor_heat_aligned_averages_readings_in_bin():
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=_make_stg_df()):
        result = stg_indoor_heat_aligned(pg_engine=MagicMock())
    df = result.value
    sensor_a = df[df["sensor_id"] == "A"].iloc[0]
    assert sensor_a["temperature_f"] == pytest.approx(72.0, abs=0.01)   # avg(71, 73)
    assert sensor_a["relative_humidity_pct"] == pytest.approx(41.0, abs=0.01)  # avg(40, 42)


def test_stg_indoor_heat_aligned_output_columns():
    with patch("orchestrator.assets.indoor_heat.pd.read_sql_query", return_value=_make_stg_df()):
        result = stg_indoor_heat_aligned(pg_engine=MagicMock())
    assert set(result.value.columns) == {
        "sensor_id", "location", "datetime_edt",
        "temperature_f", "relative_humidity_pct", "dew_point_f", "heat_index_f",
    }
```

- [ ] **Step 4.2: Run tests to verify they fail**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py -k "aligned" -v
```

Expected: FAIL with `ImportError: cannot import name 'stg_indoor_heat_aligned'`

- [ ] **Step 4.3: Add `stg_indoor_heat_aligned` to `orchestrator/assets/indoor_heat.py`**

Add after `stg_indoor_heat`:

```python
@asset(
    deps=[stg_indoor_heat],
    io_manager_key="postgres_replace",
    compute_kind="python",
    key_prefix="staging",
    group_name="staging",
)
def stg_indoor_heat_aligned(pg_engine: ResourceParam[PostgreConnResources]) -> Output[pd.DataFrame]:
    """Bin sensor readings to 20-min intervals and average per sensor per bin."""
    engine = pg_engine.create_engine()
    df = pd.read_sql_query("SELECT * FROM staging.stg_indoor_heat", engine)
    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])

    df["datetime_bin"] = df["datetime_edt"].dt.round("20min")
    aligned = (
        df.groupby(["sensor_id", "location", "datetime_bin"])
        .agg(
            temperature_f=("temperature_f", "mean"),
            relative_humidity_pct=("relative_humidity_pct", "mean"),
            dew_point_f=("dew_point_f", "mean"),
            heat_index_f=("heat_index_f", "mean"),
        )
        .reset_index()
        .rename(columns={"datetime_bin": "datetime_edt"})
    )

    return Output(
        value=aligned,
        metadata={
            "total_rows": len(aligned),
            "unique_sensors": aligned["sensor_id"].nunique(),
            "date_range_start": str(aligned["datetime_edt"].min()),
            "date_range_end": str(aligned["datetime_edt"].max()),
        },
    )
```

- [ ] **Step 4.4: Run all tests**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py -v
```

Expected: all tests pass

- [ ] **Step 4.5: Commit**

```bash
git add orchestrator/assets/indoor_heat.py orchestrator/tests/assets/test_indoor_heat.py
git commit -m "feat: add stg_indoor_heat_aligned asset with 20-min binning"
```

---

## Task 5: Add `_compute_calibration_stats` and `indoor_heat_calibration` asset

**Files:**
- Modify: `orchestrator/assets/indoor_heat.py`
- Modify: `orchestrator/tests/assets/test_indoor_heat.py`

- [ ] **Step 5.1: Write the failing tests**

Add to `orchestrator/tests/assets/test_indoor_heat.py`:

```python
from orchestrator.assets.indoor_heat import _compute_calibration_stats


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


def test_compute_calibration_stats_returns_two_dataframes():
    df = _make_aligned_df_6_sensors()
    precision_df, sensor_stats = _compute_calibration_stats(df)
    assert len(precision_df) == len(_CALIB_VARS)
    assert len(sensor_stats) == 6


def test_compute_calibration_stats_precision_has_expected_columns():
    precision_df, _ = _compute_calibration_stats(_make_aligned_df_6_sensors())
    assert set(precision_df.columns) >= {"variable", "mean_sigma", "median_sigma", "max_sigma"}


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
```

- [ ] **Step 5.2: Run tests to verify they fail**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py -k "calibration" -v
```

Expected: FAIL with `ImportError: cannot import name '_compute_calibration_stats'`

- [ ] **Step 5.3: Add `_compute_calibration_stats` to `orchestrator/assets/indoor_heat.py`**

Add after `stg_indoor_heat_aligned`:

```python
_CALIB_VARS = ["temperature_f", "relative_humidity_pct", "heat_index_f"]
_OUTLIER_SIGMA = 2.0
_EXCLUDED_SIGMA = 3.0


def _compute_calibration_stats(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Cross-sensor precision and outlier analysis.

    Returns (precision_df, sensor_stats_df).
    """
    # Per-timestamp ensemble mean across all sensors
    grouped = df.groupby("datetime_edt")[_CALIB_VARS]
    ts_mean = grouped.mean().rename(columns={v: f"{v}_mean" for v in _CALIB_VARS})
    ts_std = grouped.std().rename(columns={v: f"{v}_std" for v in _CALIB_VARS})
    ts_stats = ts_mean.join(ts_std).reset_index()

    # Precision summary: average cross-sensor σ across all timestamps
    precision_rows = []
    for var in _CALIB_VARS:
        std_col = ts_stats[f"{var}_std"].dropna()
        precision_rows.append({
            "variable": var,
            "mean_sigma": round(float(std_col.mean()), 4),
            "median_sigma": round(float(std_col.median()), 4),
            "max_sigma": round(float(std_col.max()), 4),
        })
    precision_df = pd.DataFrame(precision_rows)

    # Per-sensor bias vs ensemble mean
    ensemble = ts_stats[["datetime_edt"] + [f"{v}_mean" for v in _CALIB_VARS]]
    merged = df.merge(ensemble, on="datetime_edt", how="left")

    sensor_rows = []
    for sid, grp in merged.groupby("sensor_id"):
        row: dict = {"sensor_id": sid}
        for var in _CALIB_VARS:
            residuals = grp[var] - grp[f"{var}_mean"]
            row[f"{var}_bias"] = round(float(residuals.mean()), 4)
            row[f"{var}_std"] = round(float(residuals.std()), 4)
        sensor_rows.append(row)

    sensor_stats = pd.DataFrame(sensor_rows)
    sensor_stats["is_outlier"] = False
    sensor_stats["severity"] = "pass"

    # Flag outliers: |bias| > OUTLIER_SIGMA × std-of-biases across sensors
    for var in _CALIB_VARS:
        col = sensor_stats[f"{var}_bias"]
        sigma = col.std()
        n_sigma = col.abs() / sigma
        sensor_stats[f"{var}_n_sigma"] = round(n_sigma, 2)
        sensor_stats[f"{var}_is_outlier"] = n_sigma > _OUTLIER_SIGMA
        sensor_stats["is_outlier"] |= sensor_stats[f"{var}_is_outlier"]

    # Severity: worst n_sigma across all variables
    for idx, row in sensor_stats.iterrows():
        max_n = max(row[f"{v}_n_sigma"] for v in _CALIB_VARS)
        if max_n > _EXCLUDED_SIGMA:
            sensor_stats.at[idx, "severity"] = "excluded"
        elif max_n > _OUTLIER_SIGMA:
            sensor_stats.at[idx, "severity"] = "marginal"

    return precision_df, sensor_stats
```

- [ ] **Step 5.4: Add `indoor_heat_calibration` asset to `orchestrator/assets/indoor_heat.py`**

Add after `_compute_calibration_stats`:

```python
@asset(
    compute_kind="python",
    group_name="calibration",
)
def indoor_heat_calibration(pg_engine: ResourceParam[PostgreConnResources]) -> None:
    """Cross-sensor calibration report. Writes two tables to Postgres. Run manually only."""
    engine = pg_engine.create_engine()
    df = pd.read_sql_query("SELECT * FROM staging.indoor_heat_aligned", engine)
    df["datetime_edt"] = pd.to_datetime(df["datetime_edt"])

    precision_df, sensor_stats = _compute_calibration_stats(df)

    run_at = pd.Timestamp.now()
    precision_df["run_at"] = run_at
    sensor_stats["run_at"] = run_at

    precision_df.to_sql(
        "indoor_heat_calibration_precision", engine,
        schema="staging", if_exists="replace", index=False,
    )
    sensor_stats.to_sql(
        "indoor_heat_calibration_sensors", engine,
        schema="staging", if_exists="replace", index=False,
    )

    outlier_count = int(sensor_stats["is_outlier"].sum())
    logger.info(
        f"Calibration complete: {len(sensor_stats)} sensors, {outlier_count} outliers"
    )
```

- [ ] **Step 5.5: Run all tests**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py -v
```

Expected: all tests pass

- [ ] **Step 5.6: Commit**

```bash
git add orchestrator/assets/indoor_heat.py orchestrator/tests/assets/test_indoor_heat.py
git commit -m "feat: add _compute_calibration_stats helper and indoor_heat_calibration asset"
```

---

## Task 6: Wire jobs, register assets, update dbt model

**Files:**
- Modify: `orchestrator/jobs/indoor_heat_job.py`
- Create: `orchestrator/jobs/indoor_heat_calibration_job.py`
- Modify: `orchestrator/__init__.py`
- Modify: `warehouse/models/final/indoor_heat_daily_summary.sql`
- Modify: `warehouse/models/sources.yml`

- [ ] **Step 6.1: Update `indoor_heat_job.py` selection**

Replace the entire file content of `orchestrator/jobs/indoor_heat_job.py`:

```python
from dagster import define_asset_job

indoor_heat_job = define_asset_job(
    name="indoor_heat_job",
    selection='+key:"staging/stg_indoor_heat_aligned"+',
)
```

- [ ] **Step 6.2: Create `orchestrator/jobs/indoor_heat_calibration_job.py`**

```python
from dagster import define_asset_job

indoor_heat_calibration_job = define_asset_job(
    name="indoor_heat_calibration_job",
    selection="indoor_heat_calibration",
)
```

- [ ] **Step 6.3: Register calibration job in `orchestrator/__init__.py`**

Add the import near the other indoor heat imports:

```python
from orchestrator.jobs.indoor_heat_calibration_job import indoor_heat_calibration_job
```

Add `indoor_heat_calibration_job` to the `jobs=[...]` list in `Definitions`.

- [ ] **Step 6.4: Update `warehouse/models/final/indoor_heat_daily_summary.sql`**

Replace the entire file. Note: the IO manager writes `stg_indoor_heat_aligned` to `staging.stg_indoor_heat_aligned`, so the dbt source name must match exactly.

```sql
SELECT
    sensor_id,
    location,
    CAST(datetime_edt AS DATE)   AS reading_date,
    AVG(temperature_f)           AS avg_temperature_f,
    MAX(temperature_f)           AS max_temperature_f,
    MIN(temperature_f)           AS min_temperature_f,
    AVG(relative_humidity_pct)   AS avg_relative_humidity_pct,
    MAX(relative_humidity_pct)   AS max_relative_humidity_pct,
    MIN(relative_humidity_pct)   AS min_relative_humidity_pct,
    AVG(dew_point_f)             AS avg_dew_point_f,
    MIN(dew_point_f)             AS min_dew_point_f,
    MAX(dew_point_f)             AS max_dew_point_f,
    AVG(heat_index_f)            AS avg_heat_index_f,
    MAX(heat_index_f)            AS max_heat_index_f,
    COUNT(*)                     AS num_readings
FROM {{ source("staging", "stg_indoor_heat_aligned") }}
GROUP BY sensor_id, location, CAST(datetime_edt AS DATE)
```

- [ ] **Step 6.5: Update `warehouse/models/sources.yml`**

In the `staging` source block, make two changes:

**Update `stg_indoor_heat` columns** (°C → °F, add heat_index_f):

```yaml
      - name: stg_indoor_heat
        description: "Deduplicated heat sensor readings, normalized to °F with heat index"
        meta:
          owner: yu_cheng@mit.edu
          group: staging
        columns:
          - name: sensor_id
            description: "Sensor identifier"
          - name: location
            description: "Location identifier"
          - name: datetime_edt
            description: "Deduplicated timestamp of the sensor reading"
          - name: temperature_f
            description: "Temperature in degrees Fahrenheit"
          - name: relative_humidity_pct
            description: "Relative humidity percentage"
          - name: dew_point_f
            description: "Dew point in degrees Fahrenheit"
          - name: heat_index_f
            description: "NOAA/Rothfusz heat index in degrees Fahrenheit"
          - name: source_file
            description: "Original Dropbox filename for traceability"
          - name: last_update
            description: "Ingestion timestamp"
```

**Add `stg_indoor_heat_aligned`** entry after `stg_indoor_heat`:

```yaml
      - name: stg_indoor_heat_aligned
        description: "20-minute binned and averaged heat sensor readings, one row per sensor per bin"
        meta:
          owner: yu_cheng@mit.edu
          group: staging
        columns:
          - name: sensor_id
            description: "Sensor identifier"
          - name: location
            description: "Location identifier"
          - name: datetime_edt
            description: "20-minute bin timestamp"
          - name: temperature_f
            description: "Average temperature in degrees Fahrenheit within the bin"
          - name: relative_humidity_pct
            description: "Average relative humidity percentage within the bin"
          - name: dew_point_f
            description: "Average dew point in degrees Fahrenheit within the bin"
          - name: heat_index_f
            description: "Average NOAA/Rothfusz heat index in degrees Fahrenheit within the bin"
```

- [ ] **Step 6.6: Verify Dagster definitions load**

```bash
python -c "from orchestrator import defs; print('OK')"
```

Expected: `OK` with no errors

- [ ] **Step 6.7: Run full test suite**

```bash
python -m pytest orchestrator/tests/assets/test_indoor_heat.py -v
```

Expected: all tests pass

- [ ] **Step 6.8: Commit**

```bash
git add orchestrator/jobs/indoor_heat_job.py \
        orchestrator/jobs/indoor_heat_calibration_job.py \
        orchestrator/__init__.py \
        warehouse/models/final/indoor_heat_daily_summary.sql \
        warehouse/models/sources.yml
git commit -m "feat: wire indoor heat jobs, register calibration job, update dbt model to aligned table"
```
