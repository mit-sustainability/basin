from pathlib import Path

from dagster import Failure
import pandas as pd
import pytest

from orchestrator.assets.transit import (
    _month_range,
    _normalize_transit_monthly_summary,
    _process_transit_workbook,
)


def test_normalize_transit_monthly_summary_history():
    history = pd.DataFrame(
        [
            {
                "Local Bus": 39892,
                "Rapid Transit": 110971,
                "Total # of Taps": 182977,
                "Active_Rider": 6518,
                "Unique_Rider": 10370,
                "Active_Ratio": 0.6285438765670203,
                "Month": "2020-01",
            }
        ]
    )

    result = _normalize_transit_monthly_summary(
        history,
        source_type="historical",
        source_filename="history.csv",
        downloaded_at=pd.Timestamp("2026-04-03 00:00:00"),
    )

    assert list(result.columns) == [
        "local_bus",
        "rapid_transit",
        "total_taps",
        "active_rider",
        "unique_rider",
        "active_ratio",
        "month",
        "source_type",
        "source_filename",
        "downloaded_at",
        "statement_month",
    ]
    row = result.iloc[0]
    assert row["month"] == "2020-01"
    assert row["local_bus"] == 39892
    assert row["rapid_transit"] == 110971
    assert row["total_taps"] == 182977
    assert row["active_rider"] == 6518
    assert row["unique_rider"] == 10370
    assert row["source_type"] == "historical"
    assert row["source_filename"] == "history.csv"
    assert pd.isna(row["statement_month"])


def test_normalize_transit_monthly_summary_accepts_normalized_columns():
    history = pd.DataFrame(
        [
            {
                "local_bus": 39892,
                "rapid_transit": 110971,
                "total_taps": 182977,
                "active_rider": 6518,
                "unique_rider": 10370,
                "active_ratio": 0.6285438765670203,
                "month": "2020-01",
            }
        ]
    )

    result = _normalize_transit_monthly_summary(
        history,
        source_type="historical",
        source_filename="history.csv",
        downloaded_at=pd.Timestamp("2026-04-03 00:00:00"),
    )

    row = result.iloc[0]
    assert row["month"] == "2020-01"
    assert row["local_bus"] == 39892
    assert row["rapid_transit"] == 110971
    assert row["total_taps"] == 182977
    assert row["active_rider"] == 6518
    assert row["unique_rider"] == 10370
    assert row["source_type"] == "historical"
    assert row["source_filename"] == "history.csv"
    assert pd.isna(row["statement_month"])


def test_process_transit_workbook(tmp_path: Path):
    workbook_path = tmp_path / "invoice.xlsx"
    workbook_df = pd.DataFrame(
        {
            "Month of Use": ["10/2024", "10/2024", "10/2024"],
            "Customer Number": [1, 2, 2],
            "Total # of Taps": [4, 0, 2],
            "Local Bus": [1, 0, 3],
            "Rapid Transit": [3, 0, 1],
        }
    )
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        workbook_df.to_excel(writer, sheet_name="Invoice Detail", index=False)

    result = _process_transit_workbook(workbook_path)

    assert result == [
        {
            "local_bus": 4,
            "rapid_transit": 4,
            "total_taps": 6,
            "active_rider": 2,
            "unique_rider": 2,
            "active_ratio": 1.0,
            "month": "2024-10",
        }
    ]


def test_process_transit_workbook_groups_adjustments_by_month(tmp_path: Path):
    workbook_path = tmp_path / "invoice_adjustments.xlsx"
    workbook_df = pd.DataFrame(
        {
            "Month of Use": ["10/2024", "10/2024", "09/2024", "09/2024"],
            "Customer Number": [1, 2, 1, 3],
            "Total # of Taps": [4, 0, 2, 3],
            "Local Bus": [1, 0, 1, 2],
            "Rapid Transit": [3, 0, 1, 1],
        }
    )
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        workbook_df.to_excel(writer, sheet_name="Invoice Detail", index=False)

    result = _process_transit_workbook(workbook_path)

    assert result == [
        {
            "local_bus": 3,
            "rapid_transit": 2,
            "total_taps": 5,
            "active_rider": 2,
            "unique_rider": 2,
            "active_ratio": 1.0,
            "month": "2024-09",
        },
        {
            "local_bus": 1,
            "rapid_transit": 3,
            "total_taps": 4,
            "active_rider": 1,
            "unique_rider": 2,
            "active_ratio": 0.5,
            "month": "2024-10",
        },
    ]


def test_process_transit_workbook_skips_adjustment_separator_rows(tmp_path: Path):
    workbook_path = tmp_path / "invoice_separator.xlsx"
    workbook_df = pd.DataFrame(
        {
            "Month of Use": [
                "10/2024",
                "Adjustment for August-2024 Month",
                "08/2024",
                "08/2024",
            ],
            "Customer Number": [1, None, 2, 3],
            "Total # of Taps": [2, None, 1, 4],
            "Local Bus": [1, None, 1, 2],
            "Rapid Transit": [1, None, 0, 2],
        }
    )
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        workbook_df.to_excel(writer, sheet_name="Invoice Detail", index=False)

    result = _process_transit_workbook(workbook_path)

    assert result == [
        {
            "local_bus": 3,
            "rapid_transit": 2,
            "total_taps": 5,
            "active_rider": 2,
            "unique_rider": 2,
            "active_ratio": 1.0,
            "month": "2024-08",
        },
        {
            "local_bus": 1,
            "rapid_transit": 1,
            "total_taps": 2,
            "active_rider": 1,
            "unique_rider": 1,
            "active_ratio": 1.0,
            "month": "2024-10",
        },
    ]


def test_process_transit_workbook_requires_columns(tmp_path: Path):
    workbook_path = tmp_path / "invoice_missing.xlsx"
    workbook_df = pd.DataFrame(
        {
            "Month of Use": ["10/2024"],
            "Customer Number": [1],
            "Local Bus": [1],
            "Rapid Transit": [2],
        }
    )
    with pd.ExcelWriter(workbook_path, engine="openpyxl") as writer:
        workbook_df.to_excel(writer, sheet_name="Invoice Detail", index=False)

    with pytest.raises(Failure):
        _process_transit_workbook(workbook_path)


def test_month_range_is_inclusive():
    assert _month_range("2024-10", "2024-12") == ["2024-10", "2024-11", "2024-12"]
