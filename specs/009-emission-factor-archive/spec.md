# 009 — Emission Factor Archive

Status: draft · Branch: `emission-factors-archive` · Backlog: BL-017 – BL-021

## Why

Every Scope 3 pipeline today pins one emission factor release and one inflation
target year directly in its SQL (2021 for Supply Chain v1.2 users, 2012 for
business travel on USEEIO v2.0.1). Newer releases exist (Supply Chain v1.3,
annual EPA Emission Factors Hub updates), and we want to decide deliberately
which fiscal years use which release, per category, without editing every
pipeline each time.

The archive is the **contract** downstream pipelines join to. A pipeline asks
"for category C in fiscal year FY, what is the factor for key K, in what unit,
and in what dollar year?" and the archive answers. The pipeline no longer knows
or hardcodes a release or a target CPI year.

## Goals

1. Ingest **multiple versions** of each emission factor family side by side,
   using the same DataHub-based ingestion style as today.
2. Clean each family into one long, versioned table.
3. An **explicit, human-edited assignment** of `(category, family) -> version`
   by fiscal-year range. Nothing switches automatically when a new release lands.
4. One contract model keyed by `(category, fiscal_year, factor_key)` that
   returns factor value, unit, family, version and `dollar_year`.
5. One shared inflation helper so spend is converted to the assigned set's
   `dollar_year`, replacing the four copy-pasted CPI blocks.

## Non-goals

- Rewiring existing pipelines to the contract. That is one follow-up backlog
  item per pipeline, after this lands.
- Restating historical emissions or reporting old-vs-new deltas (separate branch).
- Pulling v1.3 / newer EPA Hub files. Development and tests use the files
  already in DataHub; adding a version later must be a registry line, not a
  redesign.

## Anti-goals (failure even if it "works")

- Breaking any current pipeline. Existing raw assets
  (`emission_factor_useeio_v2`, `emission_factor_naics`,
  `commuting_emission_factors_EPA`, `waste_emission_factors_epa`,
  `food_emission_factor`, `annual_cpi_index`) are **moved** into
  `emission_factors.py` (see Design §0) but keep their asset keys, table names
  and behavior. Their dbt sources stay untouched until each pipeline migrates.
- A new release silently changing any year's numbers.
- Parsing that depends on fixed row offsets, which break on the next EPA Hub release.

## Current state (verified in code)

| Family | Asset (file) | Version / file | Factor key | Unit | Dollar year | Consumers |
|---|---|---|---|---|---|---|
| USEEIO | `emission_factor_useeio_v2` (`construction.py`) | USEEIOv2.0.1 | USEEIO `Code` | kg CO2e / USD | 2012 | business travel |
| Supply Chain GHG EF | `emission_factor_naics` (`construction.py`) | v1.2 NAICS CO2e USD2021, merged onto USEEIO codes | USEEIO `Code` | kg CO2e / USD (with margins) | 2021 | purchased goods, construction, DOF maintenance |
| EPA Hub, mobile | `commuting_emission_factors_EPA` (`commuting.py`) | `EPA_GHG_emission_factors_2024`, `skiprows=492` | vehicle type | kg CO2e / unit (CO2e computed with the file's 100-yr GWP) | n/a | commuting 2018/2021/2023 |
| EPA Hub, waste | `waste_emission_factors_epa` (`waste.py`) | `EPA_GHG_emission_factors_June_2024` Table 9, `skiprows=425` | material × disposal method | MT CO2e / short ton | n/a | waste |
| SIMAP food | `food_emission_factor` (`food.py`) | `food_emission_factors_SIMAP.xlsx` | SIMAP category | kg CO2e / 2021 USD **and** kg CO2e / kg | 2021 (spend-based only) | food |

Inflation today: purchased goods and travel look up CPI by the transaction's
**calendar** year. Construction and DOF look it up by **fiscal** year. All of
them fall back to the latest CPI when a year is missing. `annual_cpi_index`
(`business_travel.py`) holds BLS annual-average CPI-U (`M13`) by calendar year.

## Design

### 0. Consolidate existing factor assets into `emission_factors.py`

Move these assets verbatim into `orchestrator/assets/emission_factors.py`:

| Asset | From |
|---|---|
| `emission_factor_useeio_v2`, `emission_factor_naics` | `construction.py` |
| `commuting_emission_factors_EPA` | `commuting.py` |
| `waste_emission_factors_epa` (with its asset check) | `waste.py` |
| `food_emission_factor` | `food.py` |
| `annual_cpi_index` | `business_travel.py` |

Rules:
- Same function names, so the asset keys, `group_name`, `io_manager_key`,
  `deps` and raw table names don't change. dbt sources and
  `meta.dagster.asset_key` stay untouched.
- Jobs select their inputs through dbt lineage
  (`build_dbt_asset_selection(...).upstream()`), so moved assets are still
  picked up. Verify each job's resolved selection is unchanged before and after.
- Register the new module in `orchestrator/__init__.py` with the same
  `load_assets_from_modules` pattern and include it in `Definitions`. Remove
  imports that are now unused from the source files.
- Move the factor tests out of `test_construction.py` into
  `orchestrator/tests/assets/test_emission_factors.py`, and retarget the mock
  patch paths.
- This is a pure move with no behavior change. It lands first, as its own
  commit, so later diffs stay readable.

The legacy assets and the new archive assets (§2) live side by side in this
file. Each legacy asset is deleted during its pipeline's migration.

### 1. Version registry (code)

A module-level list in `orchestrator/assets/emission_factors.py`, one entry per
release we ingest:

```python
EmissionFactorRelease(
    family="supply_chain_naics",
    version="v1.2",
    publisher="EPA",              # "EPA" or "Cornerstone"
    project="Scope3 General",
    search_term="SupplyChainGHGEmissionFactors_v1.2_NAICS_CO2e_USD2021",
    dollar_year=2021,             # None for activity-based families
    gwp_basis="AR5",              # AR5 / AR6, so methodology shifts stay visible
)
```

Filenames aren't uniform, so version metadata is declared, not parsed from names.
Adding a release means adding one entry. If its layout differs, it also needs a
per-version parser branch.

Target releases (verified 2026-11-20):

| Family | Registered now | Add when file is in DataHub |
|---|---|---|
| `supply_chain_naics` | v1.2 (EPA, USD 2021, AR5) | v1.3 (EPA, Jul 2024, USD 2022), v1.4.0 (Cornerstone, Oct 2025, USD 2024, AR6) |
| `ghg_ef_hub` | 2024 edition | 2025 (EPA, Jan 2025), 2026 (Cornerstone, Jun 2026) |
| `simap_food` | current file | none planned |

**GHG EF Hub:** one release per year, updated annually, and `version` is the
edition year. The two existing 2024 filenames (`EPA_GHG_emission_factors_2024`
used by commuting, `EPA_GHG_emission_factors_June_2024` used by waste) count as
one release. The builder confirms the mobile and waste tables match across the
two files, then registers one entry. If they differ, stop and ask.

A registered release whose file isn't found in DataHub **fails the asset**. A
version must never vanish silently.

**No USEEIO family (decided).** Supply Chain factors *are* the GHG tables
derived from USEEIO, published in a current dollar year. So the archive holds no
standalone USEEIO release, and business travel moves to `supply_chain_naics`.
The legacy `emission_factor_useeio_v2` asset still feeds `stg_travel_spending`
and the name/category columns of `emission_factor_naics`. It stays (moved, §0)
until the business travel migration item, which is larger than the others. That
item re-maps the six travel codes (481000 air, 482000 rail, 483000 ferry,
485000 ground, 721000 lodging, 722211 meals) to keys present in Supply Chain
v1.2/v1.4.0, and moves travel from 2012 USD to the assigned dollar year.

### 2. Raw archive assets (one per family)

New file `orchestrator/assets/emission_factors.py`, `group_name="raw"`,
`io_manager_key="postgres_replace"`. Each asset loops over its family's registry
entries, downloads each file the existing way (`dhub.get_project_id` +
`search_files_from_project`), parses it, tags rows with `version`,
`dollar_year` and `source_file`, and concatenates. The table is rebuilt from
the whole registry on every run, which keeps it idempotent, and still holds
every version.

| Asset / table | Grain |
|---|---|
| `raw.ef_archive_supply_chain` | version × USEEIO code (after the NAICS→USEEIO roll-up below) |
| `raw.ef_archive_ghg_ef_hub` | version × hub table × factor key (mobile vehicle type, waste material × disposal method) |
| `raw.ef_archive_simap_food` | version × SIMAP category × basis (spend, weight) |

Parsing rules:
- **EPA Hub:** find each table by its title cell, e.g. a cell starting with
  `"Table 9"`, then read the header row beneath it until the first blank row.
  Fixed `skiprows` aren't allowed. Compute CO2e with the GWP table from the **same
  file**, as `commuting.py` does now.
- **Supply Chain → USEEIO roll-up:** keep the current
  `emission_factor_naics` logic (explode `Reference USEEIO Code`, drop
  `Land Subdivision`, average per USEEIO code). Replace the
  `ref2.iloc[23, 5]` positional exclusion with the explicit NAICS title it
  currently resolves to (the builder finds it by running against the v1.2 file).
  v1.4.0 is retitled "for U.S. Commodities" and may already be keyed by
  commodity / USEEIO code. If so, its parser skips the roll-up. Either way,
  output stays keyed by USEEIO code (`factor_key_type = useeio_code`), so
  downstream mappings keep working.
- Per-family pandera models validate the output: non-null key and value,
  unique `(version, key…)`.

### 3. Unified archive model (dbt staging)

`staging.stg_emission_factor_archive`: union of the three raw tables into one
long shape:

| column | meaning |
|---|---|
| `family` | `supply_chain_naics`, `ghg_ef_hub`, `simap_food` |
| `version` | release label from registry |
| `factor_key` | the join key (USEEIO code for Supply Chain, vehicle type, `material|disposal`, SIMAP category) |
| `factor_key_type` | `useeio_code`, `vehicle_type`, `waste_material_disposal`, `simap_category` |
| `factor_basis` | `spend` or `activity` |
| `factor_value` | numeric |
| `unit` | e.g. `kg CO2e/USD`, `kg CO2e/vehicle-mile`, `MT CO2e/short ton`, `kg CO2e/kg` |
| `dollar_year` | int, null when `factor_basis = activity` |
| `publisher` | `EPA`, `Cornerstone`, … |
| `gwp_basis` | `AR5`, `AR6` |

dbt tests: `unique` on `(family, version, factor_key, factor_basis)`;
`not_null` on value and unit; `dollar_year` not null when basis is `spend`.

### 4. Assignment seed (human-edited)

`warehouse/seeds/emission_factor_assignment.csv`:

```csv
category,family,version,fy_start,fy_end
purchased_goods,supply_chain_naics,v1.2,2016,2024
purchased_goods,supply_chain_naics,v1.4.0,2025,
construction,supply_chain_naics,v1.2,2015,2024
construction,supply_chain_naics,v1.4.0,2025,
dof_maintenance,supply_chain_naics,v1.2,2019,2024
dof_maintenance,supply_chain_naics,v1.4.0,2025,
commuting,ghg_ef_hub,2024,2018,
waste,ghg_ef_hub,2024,2015,
food,simap_food,v1,2015,
```

An empty `fy_end` means open-ended. Decided policy: **fiscal years up to FY24
use v1.2 (2021 USD), and FY25 onward uses v1.4.0 (2024 USD).** Supply Chain
categories follow this policy. Business travel has no seed row yet. Its
migration item adds `business_travel,supply_chain_naics,…` rows once the travel
codes are re-mapped, and until then it reads the legacy USEEIO table. GHG EF
Hub rows get a new row per edition year when the annual update lands.

The `v1.4.0` rows are added in the same change that registers v1.4.0 (BL-018,
once the file is in DataHub). Until then the seed holds the v1.2 rows with
`fy_end=2024`.

dbt tests:
- no overlapping fiscal year ranges per `(category, family)`
- every `(family, version)` in the seed exists in `stg_emission_factor_archive`,
  so the seed can't point at a version that wasn't ingested

### 5. Contract model

`staging.emission_factor_by_fiscal_year`: expand each assignment row to one
row per fiscal year, from `fy_start` to `COALESCE(fy_end, current FY + 1)`, then
join `stg_emission_factor_archive` on `(family, version)`.

Grain and contract: **one row per `(category, fiscal_year, factor_key, factor_basis)`**
with `family`, `version`, `factor_value`, `unit`, `dollar_year`. dbt `unique`
test on that grain. This model's columns are the runtime contract, recorded in
`specs/001-platform-baseline/contracts/runtime-contracts.md`.

A downstream pipeline joins like this:

```sql
LEFT JOIN {{ ref('emission_factor_by_fiscal_year') }} AS ef
  ON ef.category = 'purchased_goods'
 AND ef.fiscal_year = t.fiscal_year
 AND ef.factor_key = t.code
 AND ef.factor_basis = 'spend'
```

### 6. Shared inflation helper

A dbt macro `inflate_to_dollar_year(amount, spend_year, dollar_year)`. It
returns `amount * cpi(dollar_year) / cpi(spend_year)` using `annual_cpi_index`,
and keeps today's fallback (latest available CPI when `spend_year` is missing).

- **Target year (decided):** always the `dollar_year` of the factor set assigned
  to that row's fiscal year. FY24 spend goes to 2021 USD (v1.2), and FY25 spend
  goes to 2024 USD (v1.4.0). Pipelines pass the contract's `dollar_year`, so no
  pipeline names a target year.
- **Spend year (Claude's call):** the calendar year of the transaction date when
  one exists, which matches the BLS annual-average CPI. Use the fiscal year only
  for sources that are already annual (construction, DOF maintenance).

## Delivery (backlog rows)

| ID | Item | Depends on |
|---|---|---|
| BL-017 | Consolidate existing factor + CPI assets into `emission_factors.py` (pure move, §0) + move tests | — |
| BL-018 | Registry + 3 raw archive assets + pandera models + unit tests (mocked DataHub, small synthetic workbooks incl. EPA title-locator) | BL-017 |
| BL-019 | dbt: sources, `stg_emission_factor_archive`, assignment seed, `emission_factor_by_fiscal_year`, tests | BL-018 |
| BL-020 | dbt macro `inflate_to_dollar_year` + parity check doc | BL-019 |
| BL-021 | Baseline doc updates (`runtime-contracts.md`, `data-model.md`), job wiring | BL-019 |
| later | One migration item per pipeline (purchased goods, construction, DOF, travel, commuting, waste, food); each deletes its legacy factor asset | BL-020 |

## Acceptance

- Archive v1.2 rows equal current `raw.emission_factor_naics`
  `"Supply Chain Emission Factors with Margins"` per USEEIO code. Same parity
  for EPA 2024 mobile/waste and SIMAP.
- For FY ≤ 2024, `emission_factor_by_fiscal_year` gives each category the same
  factor current pipelines use. For FY ≥ 2025, Supply Chain categories get
  v1.4.0 with `dollar_year = 2024`.
- Adding a fake second registry entry, for example the same v1.2 file labeled
  `v1.2-test`, yields two versions in raw and no change to the contract until
  the seed points at it.
- Existing pipelines and dbt builds unchanged and green.

## Decided by Claude (check these)

- Registry in Python, not YAML or DataHub metadata.
- One raw table per family, rebuilt from the full registry each run.
- Assignment as a dbt seed in the repo, so changes are reviewed.
- Assignment granularity is per pipeline category; per-sub-category overrides aren't supported.
- Existing factor assets are consolidated into `emission_factors.py` as a pure
  move with keys unchanged. They're removed only during per-pipeline migration.
- `annual_cpi_index` moves too, since inflation is part of the same contract.
- SIMAP food is included as a family.
- (User decision) No USEEIO archive family. Business travel moves to Supply Chain in its own, larger migration item.
- Spend year for CPI is the transaction's calendar year, or the fiscal year for already-annual sources.
- Registered-but-missing files fail the asset rather than being skipped.
- Family renamed `epa_hub` → `ghg_ef_hub`, since the 2026 edition comes from Cornerstone.

## Left to the builder

Exact pandera field names, EPA Hub table title strings per release, the
explicit NAICS title replacing `iloc[23, 5]`, and test fixture shapes.
