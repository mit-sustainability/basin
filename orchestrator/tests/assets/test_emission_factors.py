"""Unit tests for the emission factor assets"""

from unittest.mock import patch, MagicMock, Mock

import pandas as pd
import pytest
from orchestrator.assets.emission_factors import (
    emission_factor_useeio_v2,
    emission_factor_naics,
)

# Sample data to be returned by the mocked pd.read_excel call
data = {
    "1111A0/US": [0.004444844, 0.005386854],
    "1111B0/US": [0.014120387, 0.006708784],
    "111200/US": [0.006840385, 0.005020946],
    "111300/US": [0.00367557, 0.003025901],
}
sample_ef_data = pd.DataFrame(
    data,
    index=["Greenhouse Gases", "Other"],
)
sample_ef_df = sample_ef_data.loc["Greenhouse Gases"].reset_index()
sample_ef_df.columns = ["ID", "emission_factor"]
sample_code_data = pd.DataFrame(
    {
        "ID": ["1111A0/US ", "1111B0/US"],
        "Name": ["NameA", "NameB"],
        "Code": ["CodeA", "CodeB"],
        "Location": ["LocationA", "LocationB"],
        "Category": ["CatA", "CatB"],
        "Description": ["DescA", "DescB"],
    }
)
cols = ["ID", "emission_factor", "Name", "Code", "Category", "Description"]
merged_data = pd.merge(sample_ef_df, sample_code_data, on="ID", how="inner")

# Mock the DataHubResource and the pg_engine resource
mock_dhub = Mock()
mock_pg_engine = Mock()


@patch("orchestrator.assets.emission_factors.pd.ExcelFile")
@patch(
    "orchestrator.assets.emission_factors.pd.read_excel",
    side_effect=[sample_ef_data, sample_code_data],
)
@patch("orchestrator.assets.emission_factors.DataHubResource")
def test_emission_factor_useeio_v2_success(mock_dhub, mock_read_excel, mock_excel_file):
    """Test the emission_factor_useeio_v2 asset function with a successful download"""
    # Setup mock DataHubResource
    mock_dhub_instance = MagicMock()
    mock_dhub_instance.get_project_id.return_value = "test_project_id"
    mock_dhub_instance.search_files_from_project.return_value = ["test_link"]
    mock_dhub.return_value = mock_dhub_instance

    # Call the asset function
    result = emission_factor_useeio_v2(mock_dhub_instance)
    # Assertions
    mock_dhub_instance.get_project_id.assert_called_once_with("Scope3 General")
    mock_dhub_instance.search_files_from_project.assert_called_once_with("test_project_id", "USEEIOv2.0.1")
    pd.testing.assert_frame_equal(result, merged_data[cols])


@patch("orchestrator.assets.emission_factors.DataHubResource")
def test_emission_factor_useeio_v2_no_download_links_found(mock_dhub):
    """Test the exception that no files were found from Datahub"""
    # Setup mock DataHubResource
    mock_dhub_instance = MagicMock()
    mock_dhub_instance.get_project_id.return_value = "test_project_id"
    mock_dhub_instance.search_files_from_project.return_value = []
    mock_dhub.return_value = mock_dhub_instance

    # Call the asset function
    result = emission_factor_useeio_v2(mock_dhub_instance)
    # Assertions
    assert result.empty, "DataFrame is not empty"


# Sample data for mocking responses for combining EEIO and NAICS
# A different approach using pytest-mock.patch over @patch decorator
sample_download_link = ["http://example.com/naics.csv"]
sample_naics_data = pd.DataFrame(
    {
        "2017 NAICS Code": [230301, 233240, 230301, 233411],
        "2017 NAICS Title": [
            "Soybean Farming",
            "Oilseed (except Soybean) Farming",
            "Dry Pea and Bean Farming",
            "Test entry",
        ],
        "GHG": ["All GHGs", "All GHGs", "All GHGs", "All GHGs"],
        "Unit": ["kg CO2e/2021 USD, purchaser price"] * 4,
        "Supply Chain Emission Factors without Margins": [1.223, 1.223, 2.874, 3.245],
        "Margins of Supply Chain Emission Factors": [0.103, 0.103, 0.134, 0.156],
        "Supply Chain Emission Factors with Margins": [1.326, 1.326, 3.007, 2.008],
        "Reference USEEIO Code": [
            "230301,2334A0,233210,233230,233411,2332A0,233262,230302,233240,233412,2332D0",
            "2334A0,2332A0,233411,230302,233262,2332D0,233240,233412,230301,233210,233230",
            "233240,233262,233411,233230,233210,2332A0,2332D0,233412,2334A0,2332C0",
            "233411,233240,2332A0,230301,2332D0,2334A0,233210,230302,233262,233230,233412",
        ],
    }
)
sample_eeio_data = pd.DataFrame(
    {
        "ID": ["1111B0/US", "1111C0/US"],
        "emission_factor": [0.00145, 0.00123],
        "Name": ["NameB", "NameC"],
        "Code": ["230301", "233240"],
        "Category": ["CatB", "CatC"],
        "Description": ["DescB", "DescC"],
    }
)


@pytest.fixture
def setup_mocks(mocker):
    """Setup mocks for the emission_factor_naics asset function"""
    # Mock the dhub object's methods
    mocker.patch.object(mock_dhub, "get_project_id", return_value="project123")
    mocker.patch.object(mock_dhub, "search_files_from_project", return_value=sample_download_link)
    # Mock the pg_engine object's method
    mocker.patch.object(mock_pg_engine, "create_engine")
    # Mock pandas read_csv and read_sql_query
    mocker.patch("pandas.read_csv", return_value=sample_naics_data)
    mocker.patch("pandas.read_sql_query", return_value=sample_eeio_data)


def test_emission_factor_naics_no_download_links_found(setup_mocks):
    """Test the exception that no files were found from Datahub"""
    mock_dhub.search_files_from_project.return_value = []  # No download links found
    result = emission_factor_naics(mock_dhub, mock_pg_engine)
    assert result.empty, "Expected an empty DataFrame when no download links are found"


def test_emission_factor_naics_with_data(setup_mocks):
    """Test the emission_factor_naics asset function with a successful download"""
    result = emission_factor_naics(mock_dhub, mock_pg_engine)
    # Assert the result is not empty and contains expected columns
    assert not result.empty, "Expected a non-empty DataFrame"
    expected_columns = [
        "ID",
        "emission_factor",
        "Name",
        "Code",
        "Category",
        "Description",
        "Supply Chain Emission Factors without Margins",
        "Margins of Supply Chain Emission Factors",
        "Supply Chain Emission Factors with Margins",
    ]
    assert list(result.columns) == expected_columns, f"Expected columns: {expected_columns}"


def test_emission_factors_module_owns_legacy_assets():
    """All legacy factor and CPI assets live in emission_factors.py"""
    from orchestrator.assets import emission_factors as ef

    for name in [
        "emission_factor_useeio_v2",
        "emission_factor_naics",
        "commuting_emission_factors_EPA",
        "waste_emission_factors_epa",
        "food_emission_factor",
        "annual_cpi_index",
    ]:
        assert hasattr(ef, name), name


# --- Emission factor archive: registry and shared helpers -------------------

import dataclasses  # noqa: E402

from dagster import Failure  # noqa: E402

from orchestrator.assets import emission_factors as ef  # noqa: E402


def _sc_release():
    return ef.releases_for("supply_chain_naics")[0]


def _dhub_with_hits(hits):
    dhub = Mock()
    dhub.get_project_id.return_value = "pid"
    dhub.search_files_by_name.return_value = hits
    dhub.get_download_link.side_effect = lambda h: f"link-{h}"
    return dhub


def test_download_release_fails_when_missing():
    with pytest.raises(Failure, match="SupplyChainGHGEmissionFactors_v1.2.*found 0"):
        ef.download_release(_dhub_with_hits([]), _sc_release())


def test_download_release_handles_none():
    with pytest.raises(Failure, match="found 0"):
        ef.download_release(_dhub_with_hits(None), _sc_release())


def test_download_release_ignores_fuzzy_matches():
    """DataHub search is fuzzy: only an exact display_name counts"""
    rel = _sc_release()
    hits = [
        {"display_name": rel.filename + ".bak", "hash_id": "x"},
        {"display_name": rel.filename, "hash_id": "h1"},
    ]
    assert ef.download_release(_dhub_with_hits(hits), rel) == "link-h1"


def test_download_release_fails_when_ambiguous():
    rel = _sc_release()
    hits = [{"display_name": rel.filename, "hash_id": "a"}, {"display_name": rel.filename, "hash_id": "b"}]
    with pytest.raises(Failure, match="found 2"):
        ef.download_release(_dhub_with_hits(hits), rel)


def _one_row_parser(link, release):
    return pd.DataFrame(
        {
            "factor_key": ["X"],
            "factor_key_type": ["useeio_code"],
            "factor_basis": ["spend"],
            "factor_name": [None],
            "factor_value": [1.0],
            "unit": ["kg CO2e/USD"],
        }
    )


def test_build_archive_keeps_every_registered_version(monkeypatch):
    rel = _sc_release()
    monkeypatch.setattr(ef, "RELEASES", (rel, dataclasses.replace(rel, version="v1.2-test")))
    monkeypatch.setattr(ef, "download_release", lambda dhub, r: "link")
    out = ef.build_archive(Mock(), "supply_chain_naics", _one_row_parser)
    assert set(out["version"]) == {"v1.2", "v1.2-test"}
    assert list(out.columns) == ef.ARCHIVE_COLUMNS
    assert (out["dollar_year"] == 2021).all()
    assert (out["source_file"] == rel.filename).all()


def test_build_archive_keeps_parser_supplied_version_and_dollar_year(monkeypatch):
    """Multi-year files (Cornerstone EEIO) set version/dollar_year per row"""
    rel = _sc_release()
    monkeypatch.setattr(ef, "RELEASES", (rel,))
    monkeypatch.setattr(ef, "download_release", lambda dhub, r: "link")

    def parser(link, release):
        df = _one_row_parser(link, release)
        df["version"] = "custom/2025"
        df["dollar_year"] = 2025
        return df

    out = ef.build_archive(Mock(), "supply_chain_naics", parser)
    assert out.loc[0, "version"] == "custom/2025"
    assert out.loc[0, "dollar_year"] == 2025


def test_build_archive_rejects_duplicate_keys(monkeypatch):
    rel = _sc_release()
    monkeypatch.setattr(ef, "RELEASES", (rel,))
    monkeypatch.setattr(ef, "download_release", lambda dhub, r: "link")
    with pytest.raises(Exception):
        ef.build_archive(Mock(), "supply_chain_naics", lambda link, r: pd.concat([_one_row_parser(link, r)] * 2))


def test_build_archive_fails_for_unregistered_family():
    with pytest.raises(Failure, match="no registered releases"):
        ef.build_archive(Mock(), "nope", _one_row_parser)


# --- Supply Chain archive ----------------------------------------------------

SC_COLS = [
    "2017 NAICS Code",
    "2017 NAICS Title",
    "Supply Chain Emission Factors without Margins",
    "Margins of Supply Chain Emission Factors",
    "Supply Chain Emission Factors with Margins",
    "Reference USEEIO Code",
]


def _sc_frame(rows):
    """rows: (naics_code, useeio_codes, factor_with_margins)"""
    return pd.DataFrame(
        [(code, f"title {code}", f - 0.1, 0.1, f, useeio) for code, useeio, f in rows],
        columns=SC_COLS,
    )


def _rolled(df):
    out = ef.rollup_supply_chain_to_useeio(df)
    return out.set_index("Reference USEEIO Code")["Supply Chain Emission Factors with Margins"].round(6).to_dict()


def test_rollup_drops_land_subdivision_and_specialty_trades():
    df = _sc_frame([(237210, "X", 9.0), (238110, "X", 5.0), (236220, "X", 1.0)])
    assert _rolled(df) == {"X": 1.0}


def test_rollup_keeps_group_mixing_excluded_and_regular_naics():
    """v1.3 groups Land Subdivision with regular NAICS under one factor"""
    df = _sc_frame([(236116, "Y", 2.0), (237210, "Y", 2.0), (238110, "Y", 4.0)])
    assert _rolled(df) == {"Y": 2.0}


def test_rollup_averages_distinct_regular_factors():
    df = _sc_frame([(236220, "Z", 2.0), (236210, "Z", 4.0)])
    assert _rolled(df) == {"Z": 3.0}


def test_rollup_keeps_code_when_every_group_is_excluded():
    df = _sc_frame([(237210, "W", 3.0), (238110, "W", 5.0)])
    assert _rolled(df) == {"W": 4.0}


def test_rollup_explodes_comma_separated_useeio_codes():
    df = _sc_frame([(111110, "1111A0, 1111B0", 1.5)])
    assert _rolled(df) == {"1111A0": 1.5, "1111B0": 1.5}


def test_parse_supply_chain_reads_csv(mocker):
    rel = _sc_release()
    df = _sc_frame([(111110, "1111A0", 1.5)])
    df["GHG"], df["Unit"] = "All GHGs", "kg CO2e/2021 USD, purchaser price"
    mocker.patch("orchestrator.assets.emission_factors.pd.read_csv", return_value=df)
    out = ef.parse_supply_chain("link", rel)
    assert out.to_dict("records") == [
        {
            "factor_key": "1111A0",
            "factor_key_type": "useeio_code",
            "factor_basis": "spend",
            "factor_name": None,
            "factor_value": 1.5,
            "unit": "kg CO2e/USD",
        }
    ]


def test_parse_supply_chain_reads_co2e_sheet_of_xlsx(mocker):
    rel = dataclasses.replace(_sc_release(), version="v1.4.0-rc.1", filename="SupplyChainGHGEmissionFactorsv1.4.0-rc.1.xlsx")
    read_excel = mocker.patch(
        "orchestrator.assets.emission_factors.pd.read_excel", return_value=_sc_frame([(111110, "1111A0", 0.602)])
    )
    out = ef.parse_supply_chain("link", rel)
    assert read_excel.call_args.kwargs["sheet_name"] == "CO2e"
    assert out["factor_value"].tolist() == [0.602]


def test_parse_supply_chain_rejects_unexpected_layout(mocker):
    mocker.patch("orchestrator.assets.emission_factors.pd.read_csv", return_value=pd.DataFrame({"a": [1]}))
    with pytest.raises(ValueError, match="Reference USEEIO Code"):
        ef.parse_supply_chain("link", _sc_release())


# --- GHG Emission Factors Hub archive ---------------------------------------


def _grid(rows, width=8):
    """Build a header=None sheet: each row is a list placed from column 1 (column 0 holds table labels)"""
    return pd.DataFrame([list(r) + [None] * (width - len(r)) for r in rows], dtype=object)


N = None
HUB_SHEET = _grid(
    [
        [N],
        [N, "Table 1", "Stationary combustion"],
        [N, N, "Fuel", "CO2"],
        [N, N, "Coal", 1.0],
        [N],
        [N, "Table 9", "Scope 3 Category 5: Waste"],
        [N, N, "These factors are intended for..."],
        [N, N, N, "Metric Tons CO2e / Short Ton"],
        [N, N, "Material", "RecycledA", "LandfilledB", "CombustedC", "CompostedD"],
        [N, N, "Food Waste", "NA", 0.7, 0.05, 0.2],
        [N, N, "Mixed MSW", N, 0.58, 0.43, N],
        [N, N, "Source: EPA WARM"],
        [N, N, "trailing note", 99],
        [N],
        [N, "Table 10", "Scope 3 Category 6: Business Travel"],
        [N, N, "These factors are intended for..."],
        [N, N, "Vehicle Type", "CO2 Factor \n(kg CO2 / unit)", "CH4 Factor \n(g CH4 / unit)",
         "N2O Factor \n(g N2O / unit)", "Units"],
        [N, N, "Passenger Car A", 1.0, 1000.0, 0.0, "vehicle-mile"],
        [N, N, "Bus", 0.5, 0.0, 10.0, "passenger-mile"],
        [N, N, "Source: \nCO2..."],
        [N],
        [N, "Table 11", "   Global Warming Potential "],
        [N],
        [N, N, "Industrial Designation or \nCommon Name", "Chemical Formula\xa0", "100-Year GWP"],
        [N, N, "Carbon dioxide", "CO2", 1],
        [N, N, "Methane", "CH4", 28],
        [N, N, "Nitrous oxide", "N2O", 265],
        [N],
    ]
)


def test_locate_table_finds_by_exact_label_and_stops_at_source():
    t = ef.locate_table(HUB_SHEET, "Table 9", "hub.xlsx")
    assert list(t.columns) == ["Material", "RecycledA", "LandfilledB", "CombustedC", "CompostedD"]
    assert t["Material"].tolist() == ["Food Waste", "Mixed MSW"]


def test_locate_table_does_not_confuse_table_1_with_table_10():
    t = ef.locate_table(HUB_SHEET, "Table 1", "hub.xlsx")
    assert t["Fuel"].tolist() == ["Coal"]


def test_locate_table_missing_label_names_source():
    with pytest.raises(ValueError, match="'Table 99' not found in hub.xlsx"):
        ef.locate_table(HUB_SHEET, "Table 99", "hub.xlsx")


def _parse_hub(mocker):
    mocker.patch("orchestrator.assets.emission_factors.pd.read_excel", return_value=HUB_SHEET)
    rel = ef.releases_for("ghg_ef_hub")[0]
    return ef.parse_ghg_ef_hub("link", rel).set_index("factor_key")


def test_waste_melt_drops_na_and_blank_cells(mocker):
    out = _parse_hub(mocker)
    waste = out[out["factor_key_type"] == "waste_material_disposal"]
    assert "Food Waste|recycled" not in waste.index
    assert "Mixed MSW|composted" not in waste.index
    assert waste.loc["Food Waste|composted", "factor_value"] == 0.2
    assert (waste["unit"] == "MT CO2e/short ton").all()
    assert (waste["factor_basis"] == "activity").all()


def test_mobile_co2e_uses_gwp_from_same_file_and_strips_footnote(mocker):
    out = _parse_hub(mocker)
    car = out.loc["Passenger Car"]
    assert car["factor_value"] == pytest.approx(1.0 + 1000.0 * 28 / 1000)
    assert car["factor_name"] == "Passenger Car A"
    assert car["unit"] == "kg CO2e/vehicle-mile"
    assert out.loc["Bus", "factor_value"] == pytest.approx(0.5 + 10.0 * 265 / 1000)
    assert out.loc["Bus", "factor_key_type"] == "vehicle_type"


@pytest.mark.parametrize(
    "header, expected",
    [
        ("RecycledA", "recycled"),
        ("Landfilled", "landfilled"),
        ("Anaerobically Digested (Dry Digestate with Curing)E", "dry_anaerobically_digested"),
        ("Anaerobically Digested (Dry Digestate with Curing)", "dry_anaerobically_digested"),
        ("Anaerobically Digested (Wet  Digestate with Curing)", "wet_anaerobically_digested"),
    ],
)
def test_waste_disposal_names_are_stable_across_editions(header, expected):
    """2024-April and 2025 editions drop the 'E' footnote on the digestion columns"""
    assert ef.waste_disposal_name(header) == expected


# --- SIMAP food archive ------------------------------------------------------


def test_simap_emits_spend_and_weight_rows(mocker):
    sheet = pd.DataFrame(
        {
            "Food Category": ["Beef", "Chicken"],
            "Spend-Based Emissions Factor (kg CO2e/2021 USD, purchaser price)": [1.9, 0.9],
            "Weight-Based Emissions Factors (kg CO2e/kg food)": [26.45, 5.05],
        }
    )
    read_excel = mocker.patch("orchestrator.assets.emission_factors.pd.read_excel", return_value=sheet)
    out = ef.parse_simap_food("link", ef.releases_for("simap_food")[0])
    assert read_excel.call_args.kwargs["sheet_name"] == "Final Table"
    rows = {(r["factor_key"], r["factor_basis"]): (r["factor_value"], r["unit"]) for r in out.to_dict("records")}
    assert rows == {
        ("Beef", "spend"): (1.9, "kg CO2e/USD"),
        ("Chicken", "spend"): (0.9, "kg CO2e/USD"),
        ("Beef", "activity"): (26.45, "kg CO2e/kg"),
        ("Chicken", "activity"): (5.05, "kg CO2e/kg"),
    }
    assert (out["factor_key_type"] == "simap_category").all()


def test_simap_rejects_unexpected_layout(mocker):
    mocker.patch("orchestrator.assets.emission_factors.pd.read_excel", return_value=pd.DataFrame({"Food Category": ["x"]}))
    with pytest.raises(ValueError, match="Spend-Based"):
        ef.parse_simap_food("link", ef.releases_for("simap_food")[0])
