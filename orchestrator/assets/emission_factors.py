"""Emission factor and CPI assets.

Legacy single-release factor assets (moved here unchanged from construction.py,
commuting.py, waste.py, food.py and business_travel.py) live alongside the
versioned emission factor archive. See specs/009-emission-factor-archive/.
"""

from dataclasses import dataclass
import re
from datetime import datetime
from typing import Callable, Optional

import cpi
from dagster_pandera import pandera_schema_to_dagster_type
from dagster import (
    asset,
    AssetCheckResult,
    AssetCheckSpec,
    Failure,
    get_dagster_logger,
    Output,
    ResourceParam,
)
import numpy as np
import pandas as pd
import pandera as pa
from pandera.typing import Series

from orchestrator.assets.utils import normalize_column_name
from orchestrator.resources.postgres_io_manager import PostgreConnResources
from orchestrator.resources.datahub import DataHubResource


logger = get_dagster_logger()


# --- Emission factor archive: release registry ------------------------------


@dataclass(frozen=True)
class EmissionFactorRelease:
    """One published release of an emission factor family, as stored in DataHub.

    ``filename`` is the exact DataHub display name; ``search_term`` is what the
    (fuzzy) DataHub search is queried with. ``dollar_year`` is None for
    activity-based families or when the parser sets it per row.
    """

    family: str
    version: str
    publisher: str
    project: str
    search_term: str
    filename: str
    dollar_year: Optional[int]
    gwp_basis: Optional[str]


# Adding a release = adding one entry. Assignment to fiscal years lives in the
# dbt seed `emission_factor_assignment.csv`, never here.
RELEASES: tuple[EmissionFactorRelease, ...] = (
    EmissionFactorRelease(
        family="supply_chain_naics",
        version="v1.2",
        publisher="EPA",
        project="Scope3 General",
        search_term="SupplyChainGHGEmissionFactors_v1.2_NAICS_CO2e_USD2021",
        filename="SupplyChainGHGEmissionFactors_v1.2_NAICS_CO2e_USD2021.csv",
        dollar_year=2021,
        gwp_basis=None,  # not stated in the file
    ),
    EmissionFactorRelease(
        family="supply_chain_naics",
        version="v1.3.0",
        publisher="EPA",
        project="Scope3 General",
        search_term="SupplyChainGHGEmissionFactors_v1.3.0",
        filename="SupplyChainGHGEmissionFactors_v1.3.0_NAICS_CO2e_USD2022.csv",
        dollar_year=2022,
        gwp_basis=None,  # not stated in the file
    ),
    EmissionFactorRelease(
        # Release candidate; final v1.4.0 (Zenodo, Oct 2025) is in 2024 USD. Register it as its own
        # entry when uploaded, then point the seed at it.
        family="supply_chain_naics",
        version="v1.4.0-rc.1",
        publisher="Cornerstone",
        project="Scope3 General",
        search_term="SupplyChainGHGEmissionFactorsv1.4.0",
        filename="SupplyChainGHGEmissionFactorsv1.4.0-rc.1.xlsx",
        dollar_year=2023,  # unit in file: 'kg CO2e/2023 USD, purchaser price'
        gwp_basis="AR6",
    ),
    EmissionFactorRelease(
        # One file, one version per data year ('2026-rc/2024'...); dollar year = data year
        family="cornerstone_eeio",
        version="2026-rc",
        publisher="Cornerstone",
        project="Scope3 General",
        search_term="Cornerstone_EEIO_2026",
        filename="Cornerstone_EEIO_2026-rc_Spend-based_GHG_Emission_Factors.xlsx",
        dollar_year=None,
        gwp_basis="AR6",
    ),
    EmissionFactorRelease(
        family="ghg_ef_hub",
        version="2024",
        publisher="EPA",
        project="Scope3 General",
        search_term="EPA_GHG_emission_factors_June_2024",
        # June re-issue of the 2024 edition; the April file has different mobile factors
        filename="EPA_GHG_emission_factors_June_2024",
        dollar_year=None,
        gwp_basis="AR5",
    ),
    EmissionFactorRelease(
        family="ghg_ef_hub",
        version="2025",
        publisher="EPA",
        project="Scope3 General",
        search_term="hub",
        filename="ghg-emission-factors-hub-2025.xlsx",
        dollar_year=None,
        gwp_basis="AR5",
    ),
    EmissionFactorRelease(
        family="simap_food",
        version="v1",
        publisher="SIMAP",
        project="Scope3 Food",
        search_term="food_emission_factors_SIMAP.xlsx",
        filename="food_emission_factors_SIMAP.xlsx",
        dollar_year=2021,
        gwp_basis=None,
    ),
)

ARCHIVE_COLUMNS = [
    "family",
    "version",
    "publisher",
    "factor_key",
    "factor_key_type",
    "factor_basis",
    "factor_name",
    "factor_value",
    "unit",
    "dollar_year",
    "gwp_basis",
    "source_file",
]


class EmissionFactorArchive(pa.DataFrameModel):
    """Shared long schema of every raw ``ef_archive_*`` table"""

    family: Series[str]
    version: Series[str]
    publisher: Series[str]
    factor_key: Series[str]
    factor_key_type: Series[str]
    factor_basis: Series[str] = pa.Field(isin=["spend", "activity"])
    factor_name: Series[str] = pa.Field(nullable=True)
    factor_value: Series[float] = pa.Field(coerce=True)
    unit: Series[str]
    dollar_year: Series[pd.Int64Dtype] = pa.Field(nullable=True)
    gwp_basis: Series[str] = pa.Field(nullable=True)
    source_file: Series[str]

    class Config:
        unique = ["version", "factor_key", "factor_basis"]


def releases_for(family: str) -> list[EmissionFactorRelease]:
    """Registered releases of one family (reads RELEASES at call time)"""
    return [r for r in RELEASES if r.family == family]


def download_release(dhub: DataHubResource, release: EmissionFactorRelease) -> str:
    """Return the download link of exactly one DataHub file named ``release.filename``.

    Fails rather than skipping, so a registered version can never silently vanish.
    """
    project_id = dhub.get_project_id(release.project)
    hits = [f for f in (dhub.search_files_by_name(project_id, release.search_term) or [])
            if f.get("display_name") == release.filename]
    if len(hits) != 1:
        raise Failure(
            f"{release.family} {release.version}: expected 1 DataHub file named "
            f"'{release.filename}' (search '{release.search_term}'), found {len(hits)}"
        )
    return dhub.get_download_link(hits[0]["hash_id"])


def tag_release(df: pd.DataFrame, release: EmissionFactorRelease) -> pd.DataFrame:
    """Add release metadata; keep a parser-supplied ``version``/``dollar_year`` if present"""
    out = df.copy()
    out["family"] = release.family
    out["publisher"] = release.publisher
    out["gwp_basis"] = release.gwp_basis
    out["source_file"] = release.filename
    if "version" not in out:
        out["version"] = release.version
    if "dollar_year" not in out:
        out["dollar_year"] = release.dollar_year
    out["dollar_year"] = out["dollar_year"].astype("Int64")
    return out[ARCHIVE_COLUMNS]


SC_FACTOR_COLUMNS = [
    "Supply Chain Emission Factors without Margins",
    "Margins of Supply Chain Emission Factors",
    "Supply Chain Emission Factors with Margins",
]
SC_REQUIRED_COLUMNS = ["2017 NAICS Code", *SC_FACTOR_COLUMNS, "Reference USEEIO Code"]


def _is_rolled_into_other_naics(naics_code) -> bool:
    """Land Subdivision (237210) and specialty trade contractors (238*) are sub-activities
    already counted inside whole-building construction NAICS."""
    code = str(naics_code).split(".")[0]
    return code == "237210" or code.startswith("238")


def rollup_supply_chain_to_useeio(df_naics: pd.DataFrame) -> pd.DataFrame:
    """Collapse NAICS-6 Supply Chain factors onto USEEIO codes.

    Each NAICS row lists one or more ``Reference USEEIO Code``. NAICS rows sharing a USEEIO
    code and identical factors form one group. When a USEEIO code has more than one group,
    groups whose NAICS are all Land Subdivision / specialty trades are dropped (unless that
    would drop every group), and the remaining groups are averaged.
    """
    df = df_naics[SC_REQUIRED_COLUMNS].copy()
    df["Reference USEEIO Code"] = df["Reference USEEIO Code"].astype(str).str.split(",")
    df = df.explode("Reference USEEIO Code", ignore_index=True)
    df["Reference USEEIO Code"] = df["Reference USEEIO Code"].str.strip()

    keys = ["Reference USEEIO Code", *SC_FACTOR_COLUMNS]
    groups = (
        df.assign(excluded=df["2017 NAICS Code"].map(_is_rolled_into_other_naics))
        .groupby(keys, as_index=False)["excluded"]
        .all()
    )
    n_groups = groups.groupby("Reference USEEIO Code")["excluded"].transform("size")
    n_kept = groups.groupby("Reference USEEIO Code")["excluded"].transform(lambda s: (~s).sum())
    keep = (n_groups == 1) | (n_kept == 0) | ~groups["excluded"]
    return groups[keep].groupby("Reference USEEIO Code", as_index=False)[SC_FACTOR_COLUMNS].mean()


def parse_supply_chain(link: str, release: EmissionFactorRelease) -> pd.DataFrame:
    """Read a Supply Chain GHG EF release (CSV, or the ``CO2e`` sheet of an xlsx) into archive rows"""
    if release.filename.endswith(".xlsx"):
        df = pd.read_excel(link, sheet_name="CO2e", engine="openpyxl")
    else:
        df = pd.read_csv(link)
    missing = [c for c in SC_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"{release.filename}: unexpected Supply Chain layout, missing {missing}")
    rolled = rollup_supply_chain_to_useeio(df)
    return pd.DataFrame(
        {
            "factor_key": rolled["Reference USEEIO Code"],
            "factor_key_type": "useeio_code",
            "factor_basis": "spend",
            "factor_name": None,
            "factor_value": rolled["Supply Chain Emission Factors with Margins"],
            "unit": "kg CO2e/USD",
        }
    )


HUB_TABLES = {"waste": "Table 9", "mobile": "Table 10", "gwp": "Table 11"}


def waste_disposal_name(header: str) -> str:
    """Normalize a Table 9 disposal header ('RecycledA', 'Anaerobically Digested (Dry ...)E') to snake_case"""
    h = str(header).strip()
    if h.lower().startswith("anaerobically digested"):
        return "dry_anaerobically_digested" if "(dry" in h.lower() else "wet_anaerobically_digested"
    return normalize_column_name(re.sub(r"(?<=[a-z])[A-Z]$", "", h))


_FOOTNOTE = re.compile(r"\s+[A-Z]$")


def _blank(v) -> bool:
    return v is None or (isinstance(v, float) and np.isnan(v)) or (isinstance(v, str) and not v.strip())


def locate_table(sheet: pd.DataFrame, label: str, source: str) -> pd.DataFrame:
    """Extract one Hub table from a ``header=None`` sheet by its exact label cell (e.g. 'Table 9').

    The header is the first row after the label with >= 2 non-blank cells; data runs until
    the first blank row or a row whose first non-blank cell starts with 'Source'.
    """
    hits = [(i, j) for i, row in enumerate(sheet.itertuples(index=False)) for j, v in enumerate(row)
            if isinstance(v, str) and v.strip() == label]
    if not hits:
        raise ValueError(f"'{label}' not found in {source}")
    rows = sheet.values.tolist()
    i = hits[0][0] + 1
    while i < len(rows) and sum(not _blank(v) for v in rows[i]) < 2:
        i += 1
    if i == len(rows):
        raise ValueError(f"'{label}' in {source} has no header row")
    header, data = rows[i], []
    for row in rows[i + 1:]:
        cells = [v for v in row if not _blank(v)]
        if not cells or (isinstance(cells[0], str) and cells[0].strip().startswith("Source")):
            break
        data.append(row)
    keep = [j for j, h in enumerate(header) if not _blank(h)]
    return pd.DataFrame([[r[j] for j in keep] for r in data], columns=[str(header[j]).strip() for j in keep])


# Keys every Hub edition must contain; a missing one means a table was cut short or renamed.
HUB_ANCHOR_KEYS = ("Passenger Car", "Bus", "Mixed MSW|landfilled")


def _col(df: pd.DataFrame, prefix: str, source: str) -> str:
    """The one column starting with ``prefix``; a renamed header fails with the file name"""
    match = next((c for c in df.columns if c.startswith(prefix)), None)
    if match is None:
        raise ValueError(f"no column starting '{prefix}' in {source}; columns: {list(df.columns)}")
    return match


def parse_ghg_ef_hub(link: str, release: EmissionFactorRelease) -> pd.DataFrame:
    """Waste (Table 9) and business travel / commuting (Table 10) factors from one Hub edition"""
    sheet = pd.read_excel(link, sheet_name=0, header=None, engine="openpyxl")

    gwp = locate_table(sheet, HUB_TABLES["gwp"], release.filename)
    src = release.filename
    formula = gwp[_col(gwp, "Chemical Formula", src)].astype(str).str.replace("\xa0", "").str.strip()
    gwp_by_gas = dict(zip(formula, pd.to_numeric(gwp[_col(gwp, "100-Year GWP", src)], errors="coerce")))
    missing_gas = [g for g in ("CH4", "N2O") if g not in gwp_by_gas]
    if missing_gas:
        raise ValueError(f"GWP for {missing_gas} not found in {HUB_TABLES['gwp']} of {src}")

    mobile = locate_table(sheet, HUB_TABLES["mobile"], release.filename)
    co2, ch4, n2o = [pd.to_numeric(mobile[_col(mobile, p, src)], errors="coerce") for p in ("CO2", "CH4", "N2O")]
    names = mobile[_col(mobile, "Vehicle Type", src)].astype(str).str.strip()
    mobile_out = pd.DataFrame(
        {
            "factor_key": names.str.replace(_FOOTNOTE, "", regex=True),
            "factor_key_type": "vehicle_type",
            "factor_basis": "activity",
            "factor_name": names,
            "factor_value": co2 + ch4 * gwp_by_gas["CH4"] / 1000 + n2o * gwp_by_gas["N2O"] / 1000,
            "unit": "kg CO2e/" + mobile[_col(mobile, "Units", src)].astype(str).str.strip(),
        }
    )

    waste = locate_table(sheet, HUB_TABLES["waste"], release.filename)
    waste.columns = ["material", *[waste_disposal_name(c) for c in waste.columns[1:]]]
    long = waste.melt(id_vars="material", var_name="disposal", value_name="factor_value")
    long["factor_value"] = pd.to_numeric(long["factor_value"], errors="coerce")
    long = long.dropna(subset=["factor_value"])
    material = long["material"].astype(str).str.strip()
    waste_out = pd.DataFrame(
        {
            "factor_key": material + "|" + long["disposal"],
            "factor_key_type": "waste_material_disposal",
            "factor_basis": "activity",
            "factor_name": material,
            "factor_value": long["factor_value"],
            "unit": "MT CO2e/short ton",
        }
    )
    out = pd.concat([mobile_out.dropna(subset=["factor_value"]), waste_out], ignore_index=True)
    missing = [k for k in HUB_ANCHOR_KEYS if k not in set(out["factor_key"])]
    if missing:
        raise ValueError(f"{src}: expected factors {missing} not found; a Hub table may be truncated or renamed")
    return out


def parse_simap_food(link: str, release: EmissionFactorRelease) -> pd.DataFrame:
    """SIMAP food factors: one spend row (kg CO2e/USD) and one weight row (kg CO2e/kg) per category"""
    df = pd.read_excel(link, sheet_name="Final Table", engine="openpyxl")
    spend = next((c for c in df.columns if str(c).startswith("Spend-Based")), None)
    weight = next((c for c in df.columns if str(c).startswith("Weight-Based")), None)
    if spend is None or weight is None:
        raise ValueError(f"{release.filename}: unexpected SIMAP layout, need Spend-Based and Weight-Based columns")
    category = df["Food Category"].astype(str).str.strip()
    frames = [
        pd.DataFrame({"factor_basis": basis, "factor_value": df[col], "unit": unit})
        for basis, col, unit in (("spend", spend, "kg CO2e/USD"), ("activity", weight, "kg CO2e/kg"))
    ]
    out = pd.concat(frames, keys=[0, 1]).reset_index(level=0, drop=True)
    out.insert(0, "factor_key", pd.concat([category, category]))
    out.insert(1, "factor_key_type", "simap_category")
    out.insert(3, "factor_name", None)
    return out.reset_index(drop=True)


def parse_cornerstone_eeio(link: str, release: EmissionFactorRelease) -> pd.DataFrame:
    """US purchaser-price factors from every ``{YEAR}PUR`` sheet; version '<release>/<year>', dollar year = year"""
    book = pd.ExcelFile(link, engine="openpyxl")
    frames = []
    for name in book.sheet_names:
        match = re.fullmatch(r"(\d{4})PUR", name)
        if not match:
            continue
        year = int(match.group(1))
        df = pd.read_excel(book, sheet_name=name)
        us = df[df["Location"] == "USA"]
        frames.append(
            pd.DataFrame(
                {
                    "version": f"{release.version}/{year}",
                    "factor_key": us["Code"].astype(str).str.strip(),
                    "factor_key_type": "useeio_code",
                    "factor_basis": "spend",
                    "factor_name": us["Name"],
                    "factor_value": us["Total GHGs"],
                    "unit": "kg CO2e/USD",
                    "dollar_year": year,
                }
            )
        )
    if not frames:
        raise ValueError(f"{release.filename}: no {{YEAR}}PUR sheets found")
    return pd.concat(frames, ignore_index=True)


def build_archive(
    dhub: DataHubResource,
    family: str,
    parse: Callable[[str, EmissionFactorRelease], pd.DataFrame],
) -> pd.DataFrame:
    """Download, parse and tag every registered release of ``family`` into one validated table"""
    releases = releases_for(family)
    if not releases:
        raise Failure(f"{family}: no registered releases")
    frames = []
    for release in releases:
        logger.info(f"Archiving {release.family} {release.version} from {release.filename}")
        parsed = parse(download_release(dhub, release), release)
        if parsed.empty:
            raise Failure(f"{family} {release.version}: {release.filename} parsed to no rows")
        frames.append(tag_release(parsed, release))
    out = pd.concat(frames, ignore_index=True)
    return EmissionFactorArchive.validate(out)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def annual_cpi_index():
    """Get annual CPI index from python cpi library"""
    DEFAULT_SERIES_ID = cpi.defaults.DEFAULT_SERIES_ID
    logger.info("Ectract the annual CPI data using the python cpi library ")
    cpi_df = cpi.series.get_by_id(DEFAULT_SERIES_ID).to_dataframe()
    cpi_sub = cpi_df[cpi_df["period_code"] == "M13"][["year", "value", "series_id"]].sort_values("year")
    cpi_sub["last_update"] = datetime.now()

    return cpi_sub


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def emission_factor_useeio_v2(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the USEEIOv2.0.1 emission factor data from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 General")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "USEEIOv2.0.1")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the emission factor
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    coef = pd.read_excel(workbook, sheet_name="N", index_col=0)
    ef = coef.loc["Greenhouse Gases"]
    ef_df = ef.reset_index()
    ef_df.columns = ["ID", "emission_factor"]

    # Load the category to emission factor code mapping
    df_code = pd.read_excel(workbook, sheet_name="commodities_meta", index_col=0)
    merged = pd.merge(ef_df, df_code, on="ID", how="inner")
    columns = ["ID", "emission_factor", "Name", "Code", "Category", "Description"]
    return merged[columns]


@asset(
    deps=[emission_factor_useeio_v2],
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def emission_factor_naics(dhub: ResourceParam[DataHubResource], pg_engine: PostgreConnResources):
    """Ingest and combine NAICS and USEEIO emission factors for Scope3 purchased goods"""
    project_id = dhub.get_project_id("Scope3 General")
    download_links = dhub.search_files_from_project(project_id, "SupplyChainGHGEmissionFactors_v1.2_NAICS_CO2e_USD2021")
    if len(download_links) == 0:
        logger.info("No download links found!")
        return pd.DataFrame()
    # Load the NAICS_v1.2 emission factors related maintenance cost
    df_naics = pd.read_csv(download_links[0])
    df_naics.drop(columns=["GHG", "Unit"], inplace=True)

    # Load USEEIO emission factors
    engine = pg_engine.create_engine()
    df_eeio = pd.read_sql_query(
        'SELECT "ID", emission_factor, "Name", "Code", "Category", "Description" FROM raw.emission_factor_useeio_v2',
        engine,
    )

    # Collapse NAICS-6 factors onto USEEIO codes (same rule as the archive)
    ref2 = rollup_supply_chain_to_useeio(df_naics)
    factor_eeio_new = df_eeio.copy()
    factor_eeio_new = factor_eeio_new.merge(ref2, left_on="Code", right_on="Reference USEEIO Code", how="left")
    factor_eeio_new.drop(columns=["Reference USEEIO Code"], inplace=True)
    return factor_eeio_new


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def commuting_emission_factors_EPA(dhub: ResourceParam[DataHubResource]):
    """This asset ingest the Commuting Emission Factors by vehicle
    type from EPA Emission Factors Hub."""
    project_id = dhub.get_project_id("Scope3 General")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "EPA_GHG_emission_factors_2024")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the data
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    df = pd.read_excel(
        workbook,
        usecols="C:G",
        skiprows=492,
        nrows=12,
    )
    # Clean up column names
    name_map = {
        "Vehicle Type": "vehicle_type",
        "CO2 Factor \n(kg CO2 / unit)": "CO2_kg",
        "CH4 Factor \n(g CH4 / unit)": "CH4_g",
        "N2O Factor \n(g N2O / unit)": "N2O_g",
        "Units": "units",
    }
    df_out = df.rename(columns=name_map)

    # Get GWP to calculate CO2-eq
    gwp = pd.read_excel(
        workbook,
        usecols="C:E",
        skiprows=512,
        nrows=3,
    )
    gwp_100 = gwp["100-Year GWP"].to_list()
    gwp_map = dict(zip(["CO2_kg", "CH4_g", "N2O_g"], gwp_100))
    df_out["CO2eq_kg"] = (
        df_out["CO2_kg"] + df_out["CH4_g"] * gwp_map["CH4_g"] / 1000 + df_out["N2O_g"] * gwp_map["N2O_g"] / 1000
    )
    df_out["last_update"] = datetime.now()

    return df_out


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    check_specs=[
        AssetCheckSpec(
            name="waste_emission_factor_check_unique",
            asset="waste_emission_factors_epa",
        )
    ],
)
def waste_emission_factors_epa(
    dhub: ResourceParam[DataHubResource],
):
    """This asset ingest the Commuting Emission Factors by waste material streams Table 9
    from EPA Emission Factors Hub."""
    project_id = dhub.get_project_id("Scope3 General")
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, "EPA_GHG_emission_factors_June_2024")
    if len(download_links) == 0:
        logger.error("No download links found!")
        return pd.DataFrame()
    # Load the emission factors from Table 9
    workbook = pd.ExcelFile(download_links[0], engine="openpyxl")
    ef = pd.read_excel(
        workbook,
        usecols="C:I",
        skiprows=425,
        nrows=61,
    )
    mapping = {
        "RecycledA": "Recycled",
        "LandfilledB": "Landfilled",
        "CombustedC": "Combusted",
        "CompostedD": "Composted",
        "Anaerobically Digested (Dry Digestate with Curing)E": "Dry Anaerobically Digested",
        "Anaerobically Digested (Wet  Digestate with Curing)E": "Wet Anaerobically Digested",
    }
    df_out = ef.rename(columns=mapping)
    df_out.columns = [normalize_column_name(col) for col in df_out.columns]
    df_out["last_update"] = datetime.now()
    yield Output(df_out)

    # check it
    yield AssetCheckResult(passed=df_out["material"].is_unique)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def food_emission_factor(dhub: ResourceParam[DataHubResource]):
    """This asset ingests the food emission factors
    from the Data Hub"""
    project_id = dhub.get_project_id("Scope3 Food")
    logger.info(f"Found project id: {project_id}!")

    download_links = dhub.search_files_from_project(project_id, "food_emission_factors_SIMAP.xlsx")
    if len(download_links) == 0:
        logger.error("No download links found!")
    df = pd.read_excel(download_links[0], sheet_name="Final Table")
    name_mapping = {
        "Food Category": "simap_category",
        "Spend-Based Emissions Factor (kg CO2e/2021 USD, purchaser price)": "spend-based emissions factor (kg CO2e/2021 USD)",
        "Weight-Based Emissions Factors (kg CO2e/kg food)": "weight-based emissions factor (kg CO2e/kg food)",
    }
    return df.rename(columns=name_mapping)


# --- Emission factor archive: raw assets --------------------------------------

ARCHIVE_DAGSTER_TYPE = pandera_schema_to_dagster_type(EmissionFactorArchive)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=ARCHIVE_DAGSTER_TYPE,
)
def ef_archive_supply_chain(dhub: ResourceParam[DataHubResource]) -> pd.DataFrame:
    """Every registered Supply Chain GHG Emission Factors release, rolled up to USEEIO codes"""
    return build_archive(dhub, "supply_chain_naics", parse_supply_chain)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=ARCHIVE_DAGSTER_TYPE,
)
def ef_archive_ghg_ef_hub(dhub: ResourceParam[DataHubResource]) -> pd.DataFrame:
    """Every registered GHG Emission Factors Hub edition: mobile (Table 10) and waste (Table 9) factors"""
    return build_archive(dhub, "ghg_ef_hub", parse_ghg_ef_hub)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=ARCHIVE_DAGSTER_TYPE,
)
def ef_archive_simap_food(dhub: ResourceParam[DataHubResource]) -> pd.DataFrame:
    """Every registered SIMAP food emission factor release (spend- and weight-based)"""
    return build_archive(dhub, "simap_food", parse_simap_food)


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
    dagster_type=ARCHIVE_DAGSTER_TYPE,
)
def ef_archive_cornerstone_eeio(dhub: ResourceParam[DataHubResource]) -> pd.DataFrame:
    """Cornerstone EEIO spend-based factors: US, purchaser price, one version per data year"""
    return build_archive(dhub, "cornerstone_eeio", parse_cornerstone_eeio)
