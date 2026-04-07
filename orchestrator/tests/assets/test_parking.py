from dagster import Failure
import pandas as pd
import pytest

from orchestrator.assets import parking
from orchestrator.assets.parking import (
    ParkingNewbatchConfig,
    ParkingTrendConfig,
    _build_newbatch_parking_query,
    daily_parking_trend,
)


def test_build_newbatch_parking_query_uses_default_filters():
    query = _build_newbatch_parking_query(ParkingNewbatchConfig())

    assert "ENTRY_DATE >= TO_DATE('2021-06-25', 'YYYY-MM-DD')" in query
    assert "NOT IN ('SAT', 'SUN')" in query
    assert "ENTRY_DATE <=" not in query


def test_build_newbatch_parking_query_allows_optional_end_date_and_weekends():
    query = _build_newbatch_parking_query(
        ParkingNewbatchConfig(
            start_date="2024-01-01",
            end_date="2024-01-31",
            exclude_weekends=False,
        )
    )

    assert "ENTRY_DATE >= TO_DATE('2024-01-01', 'YYYY-MM-DD')" in query
    assert "ENTRY_DATE <= TO_DATE('2024-01-31', 'YYYY-MM-DD')" in query
    assert "NOT IN ('SAT', 'SUN')" not in query


def test_build_newbatch_parking_query_rejects_invalid_date_range():
    with pytest.raises(Failure):
        _build_newbatch_parking_query(ParkingNewbatchConfig(start_date="2024-02-01", end_date="2024-01-31"))


def test_daily_parking_trend_uses_prediction_end_date_from_config(monkeypatch):
    captured = {}

    class FakeProphet:
        def __init__(self, *args, **kwargs):
            captured["init_kwargs"] = kwargs

        def fit(self, ts):
            captured["fit_df"] = ts.copy()

        def predict(self, future):
            captured["future_df"] = future.copy()
            return future.assign(trend=1.0)

    monkeypatch.setattr(parking, "Prophet", FakeProphet)

    df = pd.DataFrame(
        [
            {
                "date": "2024-01-01",
                "unique_1016": 1,
                "unique_1018": 1,
                "unique_1022": 1,
                "unique_1024": 1,
                "unique_1025": 1,
                "unique_1030": 1,
                "unique_1035": 1,
                "unique_1037": 1,
                "unique_1038": 1,
                "total_unique": 8,
            },
            {
                "date": "2024-01-06",
                "unique_1016": 2,
                "unique_1018": 2,
                "unique_1022": 2,
                "unique_1024": 2,
                "unique_1025": 2,
                "unique_1030": 2,
                "unique_1035": 2,
                "unique_1037": 2,
                "unique_1038": 2,
                "total_unique": 16,
            },
        ]
    )
    holidays = pd.DataFrame(
        [
            {
                "date": "2024-01-01",
                "holiday": "New Year's Day",
                "holiday_type": "Standard Holiday",
            }
        ]
    )

    result = daily_parking_trend(
        ParkingTrendConfig(prediction_end_date="2025-01-15"),
        df,
        holidays,
    )

    assert captured["future_df"]["ds"].max() == pd.Timestamp("2025-01-15")
    assert len(result.value.index) == 1
    assert result.value["trend"].tolist() == [1.0]
