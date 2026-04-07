from unittest.mock import patch

from dagster import Failure
import pytest
from oracledb import DatabaseError

from orchestrator.resources.mit_warehouse import MITWHRSResource


def test_execute_query_raises_failure_on_database_error():
    resource = MITWHRSResource(host="warehouse.mit.edu", user="user", password="pw", sid="DWRHS")

    with patch(
        "orchestrator.resources.mit_warehouse.connect_oracledb",
        side_effect=DatabaseError("connection closed"),
    ):
        with pytest.raises(Failure, match="Fail to connect to MIT warehouse"):
            resource.execute_query("select 1 from dual", chunksize=1000)
