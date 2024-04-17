import pytest
from unittest.mock import MagicMock, patch

# Mock imports
from dagster import AssetKey, OutputContext
from pandas import DataFrame
from orchestrator.resources.postgres_io_manager import (
    PostgreSQLPandasIOManager,
)


@pytest.fixture
def mock_db_connection():
    # Mock the connection object and its methods
    mock_connection = MagicMock()
    mock_connection.execute.return_value.rowcount = 1  # Correctly return an integer for row count checks
    mock_connection.__enter__.return_value = mock_connection  # Mock the context manager
    mock_connection.__exit__.return_value = None
    return mock_connection


@pytest.fixture
def mock_engine():
    """Mock the create_engine function from sqlalchemy."""
    with patch("orchestrator.resources.postgres_io_manager.create_engine") as mock:
        mock.return_value.connect.return_value.__enter__.return_value = MagicMock()
        yield mock


@pytest.fixture
def pg_manager():
    return PostgreSQLPandasIOManager()


@pytest.fixture
def mock_dataframe():
    return MagicMock(spec=DataFrame)


@pytest.fixture
def mock_context(mocker):
    mock_context = MagicMock(spec=OutputContext)
    mock_context.asset_key = AssetKey(["public", "test_table"])
    mock_context.log = MagicMock()
    return mock_context


def test_handle_output_replace_existing(mock_db_connection, mock_dataframe, mock_context):
    """Test the handle_output method for replace mode with existing table."""
    manager = PostgreSQLPandasIOManager(write_method="replace")

    # Patch connect_postgresql method within the manager object
    with patch(
        "orchestrator.resources.postgres_io_manager.connect_postgresql",
        return_value=mock_db_connection,
    ):
        # Simulate existing table
        manager.handle_output(mock_context, mock_dataframe)

        # Assertions for replace mode
        # print(mock_db_connection.execute.call_args_list)
        mock_db_connection.execute.assert_called()
        mock_dataframe.to_sql.assert_called_once_with(
            con=mock_db_connection,
            name="test_table",
            schema="public",
            if_exists="replace",
            chunksize=500,
            index=False,
        )


def test_handle_output_append_no_existing(mock_db_connection, mock_dataframe, mock_context):
    """Test the handle_output method for append mode with no existing table."""
    manager = PostgreSQLPandasIOManager(write_method="append")
    # Patch connect_postgresql method within the manager object
    with patch(
        "orchestrator.resources.postgres_io_manager.connect_postgresql",
        return_value=mock_db_connection,
    ):
        # Simulate no existing table
        mock_db_connection.execute.return_value.rowcount = 0
        manager.handle_output(mock_context, mock_dataframe)

        mock_dataframe.to_sql.assert_called_once_with(
            con=mock_db_connection,
            name="test_table",
            schema="public",
            if_exists="append",
            chunksize=500,
            index=False,
        )


def test_handle_output_unsupported_data(pg_manager, mock_context):
    """Test the handle_output exception for unsupported data type."""
    mock_data = "some string data"  # Not a DataFrame
    with pytest.raises(Exception) as excinfo:
        pg_manager.handle_output(mock_context, mock_data)
    assert str(excinfo.value) == f"Outputs of type {type(mock_data)} not supported."


def test_load_input(mock_engine, pg_manager):
    """Test the handle_input method of the PostgreSQLPandasIOManager."""
    with patch("pandas.read_sql") as mock_read_sql:
        asset_key = AssetKey(["public", "test_table"])
        context = MagicMock(asset_key=asset_key, metadata={"columns": ["col1", "col2"]})
        pg_manager.load_input(context)
        mock_read_sql.assert_called_once()
