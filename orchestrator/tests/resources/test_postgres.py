from unittest.mock import patch, MagicMock
import pandas as pd
import pytest
from orchestrator.resources.postgres_io_manager import PostgreSQLPandasIOManager
from dagster import AssetKey


@pytest.fixture
def mock_engine():
    with patch("orchestrator.resources.postgres_io_manager.create_engine") as mock:
        # Mock the engine's connect method to return another mock
        mock.return_value.connect.return_value.__enter__.return_value = MagicMock()
        yield mock


@pytest.fixture
def io_manager():
    # You can adjust the configuration here if needed
    return PostgreSQLPandasIOManager()


@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})


def test_handle_output(mock_engine, io_manager, sample_dataframe):
    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        asset_key = AssetKey(["public", "test_table"])
        context = MagicMock(asset_key=asset_key)
        io_manager.handle_output(context, sample_dataframe)
        mock_to_sql.assert_called_once()


def test_load_input(mock_engine, io_manager):
    with patch("pandas.read_sql") as mock_read_sql:
        asset_key = AssetKey(["public", "test_table"])
        context = MagicMock(asset_key=asset_key, metadata={"columns": ["col1", "col2"]})
        io_manager.load_input(context)
        mock_read_sql.assert_called_once()
