from unittest.mock import patch, MagicMock, Mock

import pandas as pd
import pytest
from orchestrator.assets.construction import (
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


@patch("orchestrator.assets.construction.pd.ExcelFile")
@patch(
    "orchestrator.assets.construction.pd.read_excel",
    side_effect=[sample_ef_data, sample_code_data],
)
@patch("orchestrator.assets.construction.DataHubResource")
def test_emission_factor_useeio_v2_success(mock_dhub, mock_read_excel, mock_excel_file):
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


@patch("orchestrator.assets.construction.DataHubResource")
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
    # Mock the dhub object's methods
    mocker.patch.object(mock_dhub, "get_project_id", return_value="project123")
    mocker.patch.object(mock_dhub, "search_files_from_project", return_value=sample_download_link)
    # Mock the pg_engine object's method
    mocker.patch.object(mock_pg_engine, "create_engine")
    # Mock pandas read_csv and read_sql_query
    mocker.patch("pandas.read_csv", return_value=sample_naics_data)
    mocker.patch("pandas.read_sql_query", return_value=sample_eeio_data)


def test_emission_factor_naics_no_download_links_found(setup_mocks):
    mock_dhub.search_files_from_project.return_value = []  # No download links found
    result = emission_factor_naics(mock_dhub, mock_pg_engine)
    assert result.empty, "Expected an empty DataFrame when no download links are found"


def test_emission_factor_naics_with_data(setup_mocks):
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
