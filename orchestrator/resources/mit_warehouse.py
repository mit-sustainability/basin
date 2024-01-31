from contextlib import contextmanager
from typing import Iterator, Optional

from dagster import (
    ConfigurableResource,
    get_dagster_logger,
)
import oracledb
import pandas as pd
from sqlalchemy.exc import DatabaseError


logger = get_dagster_logger()


# TODO, wonder how to handle different schema for asset to write to.
@contextmanager
def connect_oracledb(config):
    # init connection, don't need this if on linux
    oracledb.init_oracle_client(lib_dir="/usr/local/opt/instantclient-basiclite/lib")
    pool = oracledb.create_pool(
        user=config.get("user"),
        password=config.get("password"),
        sid=config.get("sid"),
        host=config.get("host"),
        min=2,
        max=5,
        increment=1,
    )

    try:
        conn = pool.acquire()
        yield conn
    finally:
        if conn:
            conn.close()


class MITWHRSResource(ConfigurableResource):
    """This resource will create a postgresql connection engine."""

    host: Optional[str] = "localhost"
    port: Optional[int] = 1521
    user: Optional[str] = "sustain"
    password: Optional[str] = "test"
    sid: Optional[str] = "database"

    @property
    def _config(self):
        return self.dict()

    def execute_query(self, query: str, chunksize: int) -> list:
        """Execute a query and return a pandas dataframe."""
        try:
            with connect_oracledb(self._config) as con:
                logger.info("Successfully connect to MIT warehouse")
                # significantly speed up then pd.read_sql
                with con.cursor() as cursor:
                    cursor.arraysize = chunksize
                    cursor.execute(query)
                    out = []
                    while True:
                        rows = cursor.fetchmany(size=chunksize)  # Fetches a batch of rows
                        if not rows:
                            break
                        out.append(rows)
                    final = [item for chunk in out for item in chunk]
            return final
        except DatabaseError as e:
            logger.error(f"Fail to connect to MIT warehouse: {e}")
            return []
