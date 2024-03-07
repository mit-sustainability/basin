"""Shared objects and functions for all assets."""

import pandas as pd
import pandera as pa


def empty_dataframe_from_model(Model: pa.DataFrameModel) -> pd.DataFrame:
    """An empty dataframe model to ensure pandera check"""
    schema = Model.to_schema()
    return pd.DataFrame(columns=schema.dtypes.keys()).astype({col: str(dtype) for col, dtype in schema.dtypes.items()})
