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
import os

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


def get_postgres_env_config() -> dict[str, object]:
    return {
        "user": os.environ["PG_USER"],
        "host": os.environ["PG_WAREHOUSE_HOST"],
        "password": os.environ["PG_PASSWORD"],
        "database": os.getenv("PG_DATABASE", "postgres"),
        "port": int(os.getenv("PG_PORT", "5432")),
    }


def write_dataframe_to_table(
    *,
    config: dict[str, object],
    schema: str,
    table: str,
    obj: pd.DataFrame,
    write_method: str = "replace",
) -> None:
    if not isinstance(obj, pd.DataFrame):
        raise Exception(f"Outputs of type {type(obj)} not supported.")

    logger.info(f"schema: {schema} and table: {table}")
    logger.info(f"Write method: {write_method}")
    with connect_postgresql(config=config) as con:
        try:
            with con.begin():
                exists = (
                    con.execute(
                        text(
                            f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' AND table_schema = '{schema}';"
                        )
                    ).rowcount
                    > 0
                )
                if write_method == "replace" and exists:
                    logger.info("Clearing existing rows before replace.")
                    con.execute(text(f'DELETE FROM {schema}."{table}";'))
                if_exists = "append" if write_method != "replace" or exists else "replace"
                obj.to_sql(
                    con=con,
                    name=table,
                    schema=schema,
                    if_exists=if_exists,
                    chunksize=500,
                    index=False,
                )
        except Exception as e:
            logger.error(f"Error writing data: {e}")
            raise


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
        if obj is None:
            logger.info(
                "Skipping local Postgres write for %s.%s because the asset was materialized remotely.",
                schema,
                table,
            )
            return

        write_dataframe_to_table(
            config=self._config,
            schema=schema,
            table=table,
            obj=obj,
            write_method=self.write_method,
        )

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
