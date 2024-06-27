# Adapted from Copyright 2023 Holger Bruch (hb@mfdz.de)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, Connection
from typing import Iterator, Optional, Sequence

from dagster import (
    AssetKey,
    ConfigurableIOManager,
    ConfigurableResource,
    InputContext,
    OutputContext,
    get_dagster_logger,
)


logger = get_dagster_logger()


@contextmanager
def connect_postgresql(config) -> Iterator[Connection]:
    url = URL.create(
        "postgresql+psycopg2",
        username=config["user"],
        password=config["password"],
        host=config["host"],
        port=config["port"],
        database=config["database"],
    )
    conn = None
    engine = create_engine(url)
    with engine.begin() as precon:
        # Force create raw schema
        precon.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
    try:
        conn = engine.connect()
        yield conn
    finally:
        if conn:
            conn.close()


class PostgreSQLPandasIOManager(ConfigurableIOManager):
    """This IOManager will take in a pandas dataframe and store it in postgresql."""

    host: Optional[str] = "localhost"
    port: Optional[int] = 5432
    user: Optional[str] = "postgres"
    password: Optional[str] = "test"
    database: Optional[str] = "postgres"
    dbschema: Optional[str] = "public"
    write_method: Optional[str] = "replace"

    @property
    def _config(self):
        return self.dict()

    ## Use context to specified append or replace
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = self._get_schema_table(context.asset_key)
        logger.info(f"schema: {schema} and table: {table}")
        logger.info(f"Write method: {self.write_method}")
        if isinstance(obj, pd.DataFrame):
            # row_count = len(obj)
            # context.log.info(f"Row count: {row_count}")
            with connect_postgresql(config=self._config) as con:
                try:
                    exists = (
                        con.execute(
                            text(
                                f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' AND table_schema = '{schema}';"
                            )
                        ).rowcount
                        > 0
                    )
                    if self.write_method == "replace" and exists:
                        logger.info("Drop the table recursively.")
                        con.execute(text(f"DROP TABLE {schema}.{table} CASCADE;"))
                    obj.to_sql(
                        con=con,
                        name=table,
                        schema=schema,
                        if_exists=("append" if self.write_method != "replace" else "replace"),
                        chunksize=500,
                        index=False,
                    )
                    con.commit()
                except Exception as e:
                    logger.error(f"Error writing data: {e}")
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        schema, table = self._get_schema_table(context.asset_key)
        with connect_postgresql(config=self._config) as con:
            columns = (context.metadata or {}).get("columns")
            return self._load_input(con, table, schema, columns)

    def _load_input(
        self,
        con: Connection,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
    ) -> pd.DataFrame:
        df = pd.read_sql(
            sql=self._get_select_statement(
                table,
                schema,
                columns,
            ),
            con=con,
        )
        return df

    def _get_schema_table(self, asset_key: AssetKey):
        try:
            schema_key_prefix, table_key_suffix = asset_key.path
        except ValueError:
            schema_key_prefix = "raw"
            table_key_suffix = asset_key.path[-1]
        return schema_key_prefix, table_key_suffix

    def _get_select_statement(
        self,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
    ):
        col_str = ", ".join(columns) if columns else "*"
        return f"""SELECT {col_str} FROM {schema}.{table}"""


class PostgreConnResources(ConfigurableResource):
    """This resource will create a postgresql connection engine."""

    host: Optional[str] = "localhost"
    port: Optional[int] = 5432
    user: Optional[str] = "postgres"
    password: Optional[str] = "test"
    database: Optional[str] = "postgres"
    dbschema: Optional[str] = "public"

    def create_engine(self):
        url = URL.create(
            "postgresql+psycopg2",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )
        return create_engine(url)
