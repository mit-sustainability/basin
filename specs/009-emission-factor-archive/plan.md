# Emission Factor Archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consolidate all emission factor and CPI assets into one module, archive every release of each factor family side by side, and expose one fiscal-year-keyed contract model plus an inflation macro that downstream pipelines join to.

**Architecture:** A Python release registry drives three raw Dagster archive assets. All three share one long schema and download from DataHub the existing way. dbt unions them into `stg_emission_factor_archive`, expands a human-edited assignment seed into per-fiscal-year rows (`emission_factor_by_fiscal_year`), and provides `inflate_to_dollar_year`. Legacy assets move unchanged and stay until each pipeline migrates.

**Tech Stack:** Dagster 1.12 (`@asset`, `postgres_replace` IO manager, pandera), pandas/openpyxl, dbt-postgres + dbt_utils 1.3.1, pytest.

**Spec:** `specs/009-emission-factor-archive/spec.md`

## Global Constraints

- Legacy asset keys, function names, `group_name`, `io_manager_key`, `deps` and raw table names stay identical: `emission_factor_useeio_v2`, `emission_factor_naics`, `commuting_emission_factors_EPA`, `waste_emission_factors_epa`, `food_emission_factor`, `annual_cpi_index`.
- Existing dbt sources and pipeline models are not edited (migration is out of scope).
- Family names: `supply_chain_naics`, `ghg_ef_hub`, `simap_food`. No `useeio` family (user decision). Business travel moves to Supply Chain in its own migration item.
- Fixed `skiprows` are forbidden in new GHG EF Hub parsing; locate tables by title.
- A registered release with zero or more than one DataHub match fails the asset.
- Policy: FY ≤ 2024 → Supply Chain `v1.2` (dollar_year 2021); FY ≥ 2025 → `v1.4.0` (dollar_year 2024). v1.4.0 seed rows land only with the v1.4.0 registry entry (Task 9).
- CPI spend year = calendar year of transaction date; fiscal year only for already-annual sources. Target = contract `dollar_year`.
- Credentials and endpoints stay env-var driven (no change to resources).
- Run Python with the repo venv: `/Users/yucheng/Documents/Projects/basin/.venv/bin/python`; tests from `orchestrator/` via `DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 pytest tests/`.

## Review Focus

1. A registered release that DataHub can't find, or that matches two files (e.g. `v1.3` also matching `v1.3.1`), must fail loudly with the search term in the error. It must never yield a table missing a version. Test: Task 2.
2. When a GHG EF Hub title isn't found because the layout or wording changed, the error should name the title prefix and source file, not raise an `IndexError`. Test: Task 4.
3. Waste table cells holding `"NA"` or footnote text must be dropped, not crash numeric coercion or land as NaN factors. Test: Task 4.
4. The assignment seed pointing at a version that wasn't ingested, or having overlapping FY ranges, must fail `dbt build`. Test: Task 7.
5. When CPI is missing for the target `dollar_year`, inflation must return NULL instead of quietly using a different year. Missing *spend* year keeps today's latest-CPI fallback. Test: Task 8.

---

## File Structure

| File | Responsibility |
|---|---|
| `orchestrator/assets/emission_factors.py` (create) | Legacy factor + CPI assets (moved), release registry, shared download/tag helpers, per-family parsers, 3 archive assets |
| `orchestrator/tests/assets/test_emission_factors.py` (create) | Moved legacy tests + new archive tests |
| `orchestrator/assets/{construction,commuting,waste,food,business_travel}.py` (modify) | Remove moved assets and now-unused imports |
| `orchestrator/tests/assets/test_construction.py` (modify) | Drop moved tests (delete file if empty) |
| `orchestrator/__init__.py` (modify) | Register `emission_factors` module |
| `warehouse/models/sources.yml` (modify) | 3 new raw sources |
| `warehouse/models/staging/stg_emission_factor_archive.sql` (create) | Union of archive tables |
| `warehouse/models/staging/emission_factor_by_fiscal_year.sql` (create) | Contract model |
| `warehouse/models/staging/schema.yml` (modify) | Tests for both models |
| `warehouse/seeds/emission_factor_assignment.csv` + `seeds/schema.yml` (create/modify) | Assignment policy |
| `warehouse/tests/assert_ef_assignment_*.sql`, `assert_inflate_to_dollar_year.sql` (create) | Singular tests |
| `warehouse/macros/utils/inflate_to_dollar_year.sql` (create) | Inflation macro |
| `specs/BACKLOG.md`, `specs/001-platform-baseline/{contracts/runtime-contracts.md,data-model.md}` (modify) | Docs |

---

### Task 1: Consolidate legacy factor + CPI assets (BL-017)

**Files:**
- Create: `orchestrator/assets/emission_factors.py`, `orchestrator/tests/assets/test_emission_factors.py`
- Modify: `orchestrator/assets/construction.py`, `commuting.py`, `waste.py`, `food.py`, `business_travel.py`, `orchestrator/__init__.py`, `orchestrator/tests/assets/test_construction.py`, `specs/BACKLOG.md`

**Interfaces:**
- Produces: module `orchestrator.assets.emission_factors` exporting the six legacy assets under their current names.

- [ ] **Step 1: Snapshot job selections before the move**

Save to the session scratchpad as `job_snapshot.py`:

```python
from orchestrator import defs
JOBS = ["construction_asset_job", "commuting_asset_job", "food_asset_job",
        "waste_asset_job", "business_asset_job", "purchased_goods_job"]
for name in JOBS:
    keys = defs.resolve_job_def(name).asset_layer.executable_asset_keys
    print(name, sorted(k.to_user_string() for k in keys))
```

Run from repo root: `DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 .venv/bin/python <scratchpad>/job_snapshot.py > <scratchpad>/before.txt`
Expected: six lines, e.g. `construction_asset_job` includes `emission_factor_naics`, `emission_factor_useeio_v2`, `annual_cpi_index`.

- [ ] **Step 2: Add backlog rows BL-017..BL-021** to `specs/BACKLOG.md` from the spec's Delivery table. Mark BL-017 `in_progress`, set branch to `emission-factors-archive` and spec to `specs/009-emission-factor-archive/`. Mark the rest `ready`.

- [ ] **Step 3: Move tests first.** Move the factor tests from `test_construction.py` into `test_emission_factors.py`. Change imports to `from orchestrator.assets.emission_factors import emission_factor_useeio_v2, emission_factor_naics` and every `@patch("orchestrator.assets.construction.…")` to `orchestrator.assets.emission_factors.…`. Add:

```python
def test_emission_factors_module_owns_legacy_assets():
    from orchestrator.assets import emission_factors as ef
    for name in ["emission_factor_useeio_v2", "emission_factor_naics",
                 "commuting_emission_factors_EPA", "waste_emission_factors_epa",
                 "food_emission_factor", "annual_cpi_index"]:
        assert hasattr(ef, name)
```

- [ ] **Step 4: Run tests and verify they fail**

Run (from `orchestrator/`): `DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 ../.venv/bin/pytest tests/assets/test_emission_factors.py -v`
Expected: FAIL with `ModuleNotFoundError: orchestrator.assets.emission_factors`.

- [ ] **Step 5: Move the six assets verbatim** into `emission_factors.py`, including `waste_emission_factors_epa`'s `check_specs` and any helpers it uses (`normalize_column_name`). If a helper is shared with code that stays behind, import it from its current location rather than copying it. Delete the assets from their source files and remove imports that are now unused (`numpy` in `construction.py`, `cpi` in `business_travel.py`, etc.). The `emission_factor_naics` `deps=[emission_factor_useeio_v2]` now resolves within the same module.

- [ ] **Step 6: Register the module** in `orchestrator/__init__.py`: add `emission_factors` to the `from orchestrator.assets import (...)` list, `emission_factor_assets = load_assets_from_modules([emission_factors])`, and `+ emission_factor_assets` in `Definitions(assets=...)`.

- [ ] **Step 7: Run the full suite and snapshot again**

Run: `cd orchestrator && DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 ../.venv/bin/pytest tests/ -q` → all pass.
Run the snapshot into `after.txt`, then `diff before.txt after.txt` → no output.
Run `.venv/bin/ruff check orchestrator/assets` (if ruff is configured) → clean.

- [ ] **Step 8: Commit**

```bash
git add orchestrator/assets orchestrator/__init__.py orchestrator/tests/assets specs/BACKLOG.md
git commit -m "refactor: consolidate emission factor and CPI assets into emission_factors.py"
```

Commit only these paths. Leave the unrelated `business_travel.py` `trip_end_date` change out unless the user says otherwise. If that hunk sits in a file this task edits, stage with `git add -p` and skip that hunk.

---

### Task 2: Release registry, shared helpers, archive schema (BL-018)

**Files:**
- Modify: `orchestrator/assets/emission_factors.py`
- Test: `orchestrator/tests/assets/test_emission_factors.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class EmissionFactorRelease: family: str; version: str; publisher: str; project: str; search_term: str; dollar_year: int | None; gwp_basis: str | None`
  - `RELEASES: tuple[EmissionFactorRelease, ...]`. Initial entries: `supply_chain_naics v1.2` (EPA, `"SupplyChainGHGEmissionFactors_v1.2_NAICS_CO2e_USD2021"`, 2021, `"AR5"`), `ghg_ef_hub 2024` (EPA, search term chosen in Task 4, None, `"AR5"`), `simap_food v1` (SIMAP, project `"Scope3 Food"`, `"food_emission_factors_SIMAP.xlsx"`, 2021, None). Project is `"Scope3 General"` unless stated. There's no `useeio` family: the legacy `emission_factor_useeio_v2` asset is only moved (Task 1), never archived.
  - `releases_for(family: str) -> list[EmissionFactorRelease]`, which reads the module-level `RELEASES` at call time so tests can monkeypatch it.
  - `download_release(dhub: DataHubResource, release: EmissionFactorRelease) -> str`: returns the single download link and raises `dagster.Failure` naming `release.search_term` when there are 0 (`None` or `[]`) or more than 1 matches.
  - `ARCHIVE_COLUMNS = ["family","version","publisher","factor_key","factor_key_type","factor_basis","factor_name","factor_value","unit","dollar_year","gwp_basis","source_file"]`
  - `class EmissionFactorArchive(pa.DataFrameModel)` with those columns: `factor_key`, `factor_value`, `unit` non-null; `factor_basis` isin `["spend","activity"]`; `dataframe_check` unique on `(version, factor_key, factor_basis)`.
  - `tag_release(df: pd.DataFrame, release: EmissionFactorRelease) -> pd.DataFrame`: adds `family, version, publisher, dollar_year, gwp_basis, source_file=release.search_term` and returns `df[ARCHIVE_COLUMNS]`.
  - `build_archive(dhub, family: str, parse: Callable[[str, EmissionFactorRelease], pd.DataFrame]) -> pd.DataFrame`: for each `releases_for(family)`, calls `download_release`, then `parse`, then `tag_release`, and concatenates. Tasks 3–5 assets are thin wrappers around it.

- [ ] **Step 1: Write failing tests**

```python
SC = lambda: releases_for("supply_chain_naics")[0]

def test_download_release_fails_when_missing():
    dhub = Mock(); dhub.search_files_from_project.return_value = []
    with pytest.raises(Failure, match="SupplyChainGHGEmissionFactors_v1.2"):
        download_release(dhub, SC())

def test_download_release_fails_when_ambiguous():
    dhub = Mock(); dhub.search_files_from_project.return_value = ["a", "b"]
    with pytest.raises(Failure, match="2 files"):
        download_release(dhub, SC())

def test_download_release_handles_none():
    dhub = Mock(); dhub.search_files_from_project.return_value = None
    with pytest.raises(Failure):
        download_release(dhub, SC())

def test_build_archive_keeps_every_registered_version(monkeypatch):
    # monkeypatch RELEASES to include SC() plus dataclasses.replace(SC(), version="v1.2-test")
    # patch download_release -> "link"; parse returns one row {factor_key:"X", factor_key_type:"useeio_code",
    # factor_basis:"spend", factor_name:None, factor_value:1.0, unit:"kg CO2e/USD"}
    out = build_archive(Mock(), "supply_chain_naics", parse)
    assert set(out["version"]) == {"v1.2", "v1.2-test"}
    assert list(out.columns) == ARCHIVE_COLUMNS
    assert (out["dollar_year"] == 2021).all()
```

- [ ] **Step 2: Run** `pytest tests/assets/test_emission_factors.py -v` → new tests FAIL (ImportError).
- [ ] **Step 3: Implement** the interfaces above.
- [ ] **Step 4: Run** the whole test file → PASS.
- [ ] **Step 5: Commit** `feat: add emission factor release registry and archive helpers`

---

### Task 3: Supply Chain archive with explicit roll-up (BL-018)

**Files:** Modify `emission_factors.py`; test in `test_emission_factors.py`.

**Interfaces:**
- Consumes: `releases_for`, `download_release`, `tag_release`, `EmissionFactorArchive` (Task 2).
- Produces:
  - `EXCLUDED_NAICS_GROUPS: frozenset[str]`: `{"Land Subdivision", <the joined-title string legacy code reaches via ref2.iloc[23, 5]>}`.
  - `rollup_supply_chain_to_useeio(df_naics: pd.DataFrame) -> pd.DataFrame`: legacy explode/dedupe/freq/NAICS-join/average logic, excluding rows whose `NAICS` is in `EXCLUDED_NAICS_GROUPS`. It returns columns `["Reference USEEIO Code", <the numeric factor columns legacy keeps>]`, including `"Supply Chain Emission Factors with Margins"`. Legacy `emission_factor_naics` now calls it. Remove the stray `print(len(ref2))`.
  - `parse_supply_chain(link: str, release) -> pd.DataFrame` in archive shape: `factor_key=Reference USEEIO Code`, `factor_key_type="useeio_code"`, `factor_basis="spend"`, `factor_value="Supply Chain Emission Factors with Margins"`, `unit="kg CO2e/USD"`, `factor_name=None`. It dispatches on `release.version`: `v1.2` reads CSV, drops `GHG`/`Unit`, rolls up. Unknown versions raise `NotImplementedError(version)` until Task 9 adds them.
  - Asset `ef_archive_supply_chain(dhub) -> pd.DataFrame` = `build_archive(dhub, "supply_chain_naics", parse_supply_chain)`, with `group_name="raw"`, `io_manager_key="postgres_replace"`, and dagster type from `EmissionFactorArchive`. Tasks 4 and 5 follow the same pattern.

- [ ] **Step 1: Find the excluded group.** With DataHub creds, run the legacy logic against the v1.2 file up to `explode` and print `ref2.iloc[23, 5]`. Paste the exact string into `EXCLUDED_NAICS_GROUPS`. (If DataHub is unavailable, read `raw.emission_factor_naics` and stop to ask; don't guess.)
- [ ] **Step 2: Write failing tests**
  - `test_rollup_excludes_named_groups`: synthetic `df_naics` where USEEIO code `X` maps to two NAICS rows (`"Land Subdivision"` = 9.0, `"Other"` = 1.0). Assert the rolled-up `X` value == 1.0.
  - `test_rollup_averages_shared_code`: two non-excluded NAICS rows for code `Y` (2.0, 4.0) → 3.0.
  - `test_parse_supply_chain_rejects_unknown_version`: `NotImplementedError` for `"v9.9"`.
  - The existing `test_emission_factor_naics_with_data` still passes unchanged.
- [ ] **Step 3: Run** → new tests FAIL.
- [ ] **Step 4: Implement**, then run the file → PASS.
- [ ] **Step 5: Parity check (manual, needs warehouse).** Materialize `ef_archive_supply_chain` locally, then:

```sql
SELECT count(*) FROM raw.emission_factor_naics n
JOIN raw.ef_archive_supply_chain a ON a.factor_key = n."Code" AND a.version = 'v1.2'
WHERE abs(a.factor_value - n."Supply Chain Emission Factors with Margins") > 1e-12;
```

Expected: `0`. Also check that the archive's row count equals the count of non-null legacy factors.
- [ ] **Step 6: Commit** `feat: add supply chain archive asset with explicit NAICS exclusions`

---

### Task 4: GHG EF Hub archive with title locator (BL-018)

**Files:** Modify `emission_factors.py`; test in `test_emission_factors.py`.

**Interfaces:**
- Produces:
  - `locate_table(sheet: pd.DataFrame, title_prefix: str, source: str) -> pd.DataFrame`. `sheet` is read with `header=None`. It finds the first cell whose stripped string starts with `title_prefix`. The header is the first following row with ≥2 non-null cells, and data rows run until the first all-null row. All-null columns are dropped. It raises `ValueError(f"'{title_prefix}' not found in {source}")` when the title is absent.
  - `HUB_TABLES: dict[str, str]`: `{"mobile": <title prefix of the table commuting reads>, "waste": "Table 9", "gwp": <title prefix of the GWP table>}`. Read the exact prefixes from the 2024 file at the regions the legacy offsets hit (commuting `skiprows=492`, waste `skiprows=425`, GWP `skiprows=512`).
  - `parse_ghg_ef_hub(link: str, release) -> pd.DataFrame` in archive shape:
    - **mobile:** `factor_key=vehicle_type`, `factor_key_type="vehicle_type"`, `factor_basis="activity"`, `factor_value=CO2eq_kg` (same formula as legacy `commuting_emission_factors_EPA`, GWPs from the same file), `unit="kg CO2e/" + units`.
    - **waste:** melt the disposal columns (same rename map as legacy `waste_emission_factors_epa`, normalized), `factor_key=f"{material}|{disposal}"`, `factor_key_type="waste_material_disposal"`, `factor_basis="activity"`, `unit="MT CO2e/short ton"`. Coerce with `pd.to_numeric(errors="coerce")` and drop NaN.
  - Asset `ef_archive_ghg_ef_hub(dhub)`.

- [ ] **Step 1: Confirm one 2024 release.** Parse both `EPA_GHG_emission_factors_2024` and `EPA_GHG_emission_factors_June_2024` with `locate_table` and compare the mobile and waste tables. If they're identical, register the one waste uses (`…June_2024`) as `ghg_ef_hub 2024`. If they differ, stop and ask the user.
- [ ] **Step 2: Write failing tests** on a synthetic sheet grid (a `pd.DataFrame` of objects with blank rows, a title row, a header row and data):
  - `test_locate_table_finds_by_title`: data rows match and the trailing note after a blank row is excluded.
  - `test_locate_table_missing_title_names_source`: `pytest.raises(ValueError, match="Table 9.*hub.xlsx")`.
  - `test_waste_melt_drops_na_cells`: a cell `"NA"` produces no row, and a numeric cell produces `factor_key == "Food Waste|composted"`.
  - `test_mobile_co2e_uses_file_gwp`: with CO2=1 kg, CH4=1000 g, N2O=0, GWP CH4=28 → `factor_value == 29.0`.
- [ ] **Step 3: Run** → FAIL. **Step 4: Implement**, run → PASS.
- [ ] **Step 5: Parity check (manual):** archive `2024` mobile values equal `raw."commuting_emission_factors_EPA"."CO2eq_kg"` per vehicle type, and waste values equal `raw.waste_emission_factors_epa` per material × disposal. Expected: 0 mismatches.
- [ ] **Step 6: Commit** `feat: add GHG EF Hub archive asset with title-based table lookup`

---

### Task 5: SIMAP food archive (BL-018)

**Files:** Modify `emission_factors.py`; test in `test_emission_factors.py`.

**Interfaces:**
- Produces: `parse_simap_food(link: str, release) -> pd.DataFrame` that reads sheet `"Final Table"` and emits two rows per `Food Category`:
  - spend: `factor_basis="spend"`, `unit="kg CO2e/USD"`, value from the `Spend-Based Emissions Factor (kg CO2e/2021 USD, purchaser price)` column
  - weight: `factor_basis="activity"`, `unit="kg CO2e/kg"`, value from `Weight-Based Emissions Factors (kg CO2e/kg food)`
  - both: `factor_key_type="simap_category"`

  Asset `ef_archive_simap_food(dhub)`.

- [ ] **Step 1: Test** `test_simap_emits_spend_and_weight_rows`: a two-category fixture gives 4 rows, values map to the right basis, and every row has `dollar_year == 2021` (nulling for activity rows happens in dbt, Task 6).
- [ ] **Step 2: Run** → FAIL. **Step 3: Implement**, run → PASS.
- [ ] **Step 4: Run the full suite** `pytest tests/ -q` → PASS. Update BL-018 status in `specs/BACKLOG.md` to `done`, noting that the v1.3/v1.4.0/hub 2025–2026 entries are pending Task 9.
- [ ] **Step 5: Commit** `feat: add SIMAP food archive asset`

---

### Task 6: dbt sources + `stg_emission_factor_archive` (BL-019)

**Files:** Modify `warehouse/models/sources.yml` and `warehouse/models/staging/schema.yml`; create `warehouse/models/staging/stg_emission_factor_archive.sql`.

**Interfaces:**
- Consumes: raw tables `ef_archive_supply_chain`, `ef_archive_ghg_ef_hub`, `ef_archive_simap_food` (columns = `ARCHIVE_COLUMNS`).
- Produces: `ref('stg_emission_factor_archive')` with `ARCHIVE_COLUMNS` minus `source_file`, where `dollar_year` is `CASE WHEN factor_basis = 'spend' THEN dollar_year END`.

- [ ] **Step 1: Sources.** Add the three tables under the `raw` source, each with `meta.dagster.asset_key: ["<asset name>"]`, matching the existing entries.
- [ ] **Step 2: Tests in `schema.yml`:**
  - `dbt_utils.unique_combination_of_columns` on `[family, version, factor_key, factor_basis]`
  - `not_null` on `factor_value`, `unit`, `family`, `version`
  - `dbt_utils.expression_is_true` with `expression: "factor_basis <> 'spend' OR dollar_year IS NOT NULL"`
  - `accepted_values` on `factor_basis` with `['spend','activity']`
- [ ] **Step 3: Write the model:** `UNION ALL` of the three sources selecting explicit columns, `materialized='view'` (the staging default).
- [ ] **Step 4: Run** `uv run dbt build --project-dir warehouse --select stg_emission_factor_archive` (after materializing the three raw assets locally) → all tests PASS.
- [ ] **Step 5: Commit** `feat(dbt): add unified emission factor archive model`

---

### Task 7: Assignment seed + contract model (BL-019)

**Files:**
- Create: `warehouse/seeds/emission_factor_assignment.csv`, `warehouse/models/staging/emission_factor_by_fiscal_year.sql`, `warehouse/tests/assert_ef_assignment_no_overlap.sql`, `warehouse/tests/assert_ef_assignment_versions_ingested.sql`
- Modify: `warehouse/seeds/schema.yml`, `warehouse/models/staging/schema.yml`

**Interfaces:**
- Consumes: `ref('stg_emission_factor_archive')`, macro `fiscal_year()`.
- Produces: `ref('emission_factor_by_fiscal_year')` with columns `category, fiscal_year, factor_key, factor_key_type, factor_basis, family, version, publisher, factor_value, unit, dollar_year, gwp_basis`. Grain is `(category, fiscal_year, factor_key, factor_basis)`. This is the runtime contract.

- [ ] **Step 1: Seed.** Use the spec §4 CSV **without** the three `v1.4.0` rows: v1.2 rows have `fy_end=2024`, the others are open-ended. Add `seeds/schema.yml` `config.column_types` with `fy_start: integer` and `fy_end: integer`, plus `not_null` on `category, family, version, fy_start`.
- [ ] **Step 2: Singular tests.** Each returns rows on failure.
  - `assert_ef_assignment_no_overlap.sql`: self-join on `(category, family)` with different rows where `a.fy_start <= COALESCE(b.fy_end, 9999) AND b.fy_start <= COALESCE(a.fy_end, 9999)`.
  - `assert_ef_assignment_versions_ingested.sql`: seed `(family, version)` pairs with no matching `stg_emission_factor_archive` row.
- [ ] **Step 3: Contract model.** `materialized='table'`. Expand each seed row with `generate_series(fy_start, COALESCE(fy_end, {{ fiscal_year('current_date') }} + 1))`, then inner join the archive on `(family, version)`. Add `dbt_utils.unique_combination_of_columns` on the grain and `not_null` on `factor_value, unit`.
- [ ] **Step 4: Prove the tests bite.** Temporarily add the row `purchased_goods,supply_chain_naics,v1.2,2020,` to the seed, run `dbt build --select emission_factor_assignment+`, and expect `assert_ef_assignment_no_overlap` to FAIL. Then change that row's version to `v0.0` and expect `assert_ef_assignment_versions_ingested` to FAIL as well. Revert the row.
- [ ] **Step 5: Run** `uv run dbt build --project-dir warehouse --select emission_factor_assignment+` → PASS. Spot check:

```sql
SELECT DISTINCT category, fiscal_year, version, dollar_year
FROM staging.emission_factor_by_fiscal_year
WHERE category = 'purchased_goods' AND fiscal_year IN (2024, 2025);
```

Expected: 2024 → `v1.2 / 2021`. FY2025 has no row yet, since v1.4.0 isn't registered and the v1.2 range ends at 2024. Purchased goods FY25 has no factors until Task 9.
- [ ] **Step 6: Commit** `feat(dbt): add emission factor assignment seed and fiscal-year contract model`

---

### Task 8: `inflate_to_dollar_year` macro (BL-020)

**Files:** Create `warehouse/macros/utils/inflate_to_dollar_year.sql` and `warehouse/tests/assert_inflate_to_dollar_year.sql`.

**Interfaces:**
- Produces: `{{ inflate_to_dollar_year(amount, spend_year, dollar_year) }}`, a SQL expression equal to `amount * cpi(dollar_year) / COALESCE(cpi(spend_year), latest_cpi)`. `cpi(y)` is a scalar subquery on `{{ source('raw','annual_cpi_index') }}.value WHERE year = y`. When `cpi(dollar_year)` is missing, the result is NULL.
- Docstring in the macro header: spend year = calendar year of the transaction date; fiscal year only for already-annual sources. Target = the contract's `dollar_year`.

- [ ] **Step 1: Singular test** (returns rows on failure). Select from a `VALUES` CTE of cases and compare against expected values computed inline from the source:
  - `(100, 2021, 2021)` → `100`
  - `(100, 2012, 2021)` → `100 * cpi2021 / cpi2012`
  - `(100, 3000, 2021)` → `100 * cpi2021 / latest_cpi` (missing spend year falls back)
  - `(100, 2021, 3000)` → `NULL` (missing target year)
- [ ] **Step 2: Run** `dbt test --select assert_inflate_to_dollar_year` → FAIL (macro undefined).
- [ ] **Step 3: Implement the macro.** **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** `feat(dbt): add inflate_to_dollar_year macro`

---

### Task 9: Register new releases (BL-018 follow-up, blocked on user uploading files)

**Files:** `emission_factors.py`, tests, `warehouse/seeds/emission_factor_assignment.csv`.

- [ ] **Step 1:** Confirm the user has uploaded the v1.3, v1.4.0, Hub 2025 and Hub 2026 files to DataHub, and get their exact filenames.
- [ ] **Step 2:** Add `RELEASES` entries:
  - `supply_chain_naics v1.3`: EPA, dollar_year 2022, AR5
  - `supply_chain_naics v1.4.0`: Cornerstone, dollar_year 2024, AR6
  - `ghg_ef_hub 2025`: EPA, AR5
  - `ghg_ef_hub 2026`: Cornerstone, AR6

  Use search terms specific enough that each matches exactly one file (Task 2 enforces this). Verify each `dollar_year` and `gwp_basis` against the file's own documentation sheet.
- [ ] **Step 3:** Inspect the v1.3/v1.4.0 columns. If they're keyed by USEEIO/commodity code already, add a `parse_supply_chain` branch that skips the roll-up. Otherwise reuse it. Add one test per new branch on a synthetic frame shaped like the real header row. Also query which of the six travel codes (481000, 482000, 483000, 485000, 721000, 722211) exist as `factor_key` in v1.2 and v1.4.0, and write the result into the business travel migration backlog row so that item starts with the re-mapping known.
- [ ] **Step 4:** If the Hub 2025/2026 titles differ from 2024, change `HUB_TABLES` to a per-version mapping. Add a test per changed title.
- [ ] **Step 5:** Add the three `v1.4.0` seed rows with `fy_start=2025`. Run `dbt build --select emission_factor_assignment+` → PASS. The spot check now shows FY2025 → `v1.4.0 / 2024`.
- [ ] **Step 6: Commit** `feat: register supply chain v1.3/v1.4.0 and GHG EF Hub 2025/2026 releases`

---

### Task 10: Baseline docs (BL-021)

**Files:** `specs/001-platform-baseline/contracts/runtime-contracts.md`, `specs/001-platform-baseline/data-model.md`, `specs/BACKLOG.md`, `specs/009-emission-factor-archive/spec.md` (status).

- [ ] **Step 1:** In `runtime-contracts.md`, add an "Emission factor contract" section covering:
  - the `emission_factor_by_fiscal_year` grain and columns (copy from Task 7)
  - the `inflate_to_dollar_year` semantics (Task 8)
  - the example join from spec §5
  - the rule that pipelines never hardcode a release or target year
- [ ] **Step 2:** In `data-model.md`, list the three `raw.ef_archive_*` tables, the seed, and the two staging models. Note that the legacy factor tables now live in `emission_factors.py` and are slated for removal per pipeline migration.
- [ ] **Step 3:** Mark the backlog rows done except those blocked on Task 9. Add one `proposed` backlog row per pipeline migration (purchased goods, construction, DOF, business travel, commuting, waste, food). The business travel row is larger than the others: it re-maps the six USEEIO travel codes to Supply Chain keys, moves travel from 2012 USD to the assigned dollar year, adds its seed rows, and retires `emission_factor_useeio_v2`. It also replaces the USEEIO name/category columns `emission_factor_naics` takes from that table. Set the spec status to `implemented` (or `partial` if Task 9 is pending).
- [ ] **Step 4: Commit** `docs: record emission factor archive contract in platform baseline`

---

## Execution Order

Tasks 1 → 2 → 3/4/5 (independent of each other once Task 2 lands) → 6 → 7 → 8 → 10. Task 9 runs whenever the files are in DataHub, after Task 7.
