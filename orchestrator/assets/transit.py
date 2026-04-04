from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path
import tempfile
from typing import Optional
from urllib.parse import urlparse

from dagster import (
    Config,
    Failure,
    MetadataValue,
    Output,
    ResourceParam,
    asset,
    get_dagster_logger,
)
import pandas as pd

from orchestrator.assets.utils import add_dhub_sync
from orchestrator.resources.datahub import DataHubResource
from orchestrator.resources.playwright import PlaywrightBrowserResource


logger = get_dagster_logger()

TRANSIT_PROJECT_NAME = "Commuter MBTA"
TRANSIT_HISTORY_SEARCH_TERM = "historical_transit_monthly"
TRANSIT_DOWNLOAD_URL = "https://passprogram.mbta.com"
TRANSIT_BILLING_HISTORY_URL = "https://mobility.mbta.com/Company/Billing/BillingHistory.aspx"
TRANSIT_LOGIN_USERNAME_SELECTOR = "#ctl00_LoginView1_UserLogin1_Login1_UserName"
TRANSIT_LOGIN_PASSWORD_SELECTOR = "#ctl00_LoginView1_UserLogin1_Login1_Password"
TRANSIT_LOGIN_BUTTON_SELECTOR = "#ctl00_LoginView1_UserLogin1_Login1_LoginButton"
TRANSIT_BILLING_TABLE_SELECTOR = 'th[scope="col"]:has-text("Month Of Use")'
TRANSIT_DOWNLOAD_BUTTON_SELECTOR = "#lbDownloadDetail"
TRANSIT_PORTAL_TIMEOUT_MS = 20_000
TRANSIT_REQUIRED_SOURCE_COLUMNS = [
    "Local Bus",
    "Rapid Transit",
    "Total # of Taps",
    "Active_Rider",
    "Unique_Rider",
    "Active_Ratio",
    "Month",
]
TRANSIT_REQUIRED_WORKBOOK_COLUMNS = {
    "Month of Use",
    "Customer Number",
    "Total # of Taps",
    "Local Bus",
    "Rapid Transit",
}
TRANSIT_SOURCE_RENAME_MAP = {
    "Local Bus": "local_bus",
    "Rapid Transit": "rapid_transit",
    "Total # of Taps": "total_taps",
    "Active_Rider": "active_rider",
    "Unique_Rider": "unique_rider",
    "Active_Ratio": "active_ratio",
    "Month": "month",
}
TRANSIT_NORMALIZED_COLUMNS = [
    "local_bus",
    "rapid_transit",
    "total_taps",
    "active_rider",
    "unique_rider",
    "active_ratio",
    "month",
]


class TransitMonthlyConfig(Config):
    start_month: str
    end_month: str | None = None


@dataclass(frozen=True)
class DownloadedTransitWorkbook:
    month: str
    file_path: Path


def _extract_filename(download_link: str, fallback_name: str) -> str:
    parsed = urlparse(download_link)
    name = Path(parsed.path).name
    return name or fallback_name


def _normalize_month_string(value: str) -> str:
    parsed = pd.to_datetime(value, format="%Y-%m", errors="raise")
    return parsed.strftime("%Y-%m")


def _month_range(start_month: str, end_month: str | None = None) -> list[str]:
    start = pd.Period(_normalize_month_string(start_month), freq="M")
    end = pd.Period(_normalize_month_string(end_month or start_month), freq="M")
    if end < start:
        raise Failure("Transit month range is invalid: `end_month` must be on or after `start_month`.")
    return [period.strftime("%Y-%m") for period in pd.period_range(start=start, end=end, freq="M")]


def _portal_month_candidates(month: str) -> list[str]:
    dt = pd.Period(month, freq="M").to_timestamp()
    return [
        dt.strftime("%m/%Y"),
        dt.strftime("%m-01-%Y"),
        dt.strftime("%m/%d/%Y"),
        month,
    ]


def _normalize_transit_monthly_summary(
    df: pd.DataFrame,
    *,
    source_type: str,
    source_filename: str,
    downloaded_at: datetime,
) -> pd.DataFrame:
    if set(TRANSIT_REQUIRED_SOURCE_COLUMNS).issubset(df.columns):
        normalized = df.loc[:, TRANSIT_REQUIRED_SOURCE_COLUMNS].rename(columns=TRANSIT_SOURCE_RENAME_MAP)
    elif set(TRANSIT_NORMALIZED_COLUMNS).issubset(df.columns):
        normalized = df.loc[:, TRANSIT_NORMALIZED_COLUMNS].copy()
    else:
        missing_source_columns = set(TRANSIT_REQUIRED_SOURCE_COLUMNS) - set(df.columns)
        missing_normalized_columns = set(TRANSIT_NORMALIZED_COLUMNS) - set(df.columns)
        raise Failure(
            "Transit monthly data is missing required columns. "
            f"Expected either source columns {sorted(TRANSIT_REQUIRED_SOURCE_COLUMNS)} "
            f"or normalized columns {sorted(TRANSIT_NORMALIZED_COLUMNS)}. "
            f"Missing source columns: {sorted(missing_source_columns)}. "
            f"Missing normalized columns: {sorted(missing_normalized_columns)}."
        )

    normalized = normalized.copy()
    normalized["month"] = normalized["month"].map(_normalize_month_string)
    integer_columns = [
        "local_bus",
        "rapid_transit",
        "total_taps",
        "active_rider",
        "unique_rider",
    ]
    normalized[integer_columns] = normalized[integer_columns].fillna(0).astype(int)
    normalized["active_ratio"] = normalized["active_ratio"].astype(float)
    normalized["source_type"] = source_type
    normalized["source_filename"] = source_filename
    normalized["downloaded_at"] = downloaded_at
    normalized["statement_month"] = pd.Series([pd.NA] * len(normalized), dtype="string")
    normalized = normalized.sort_values("month").drop_duplicates(subset=["month"], keep="last")
    return normalized.reset_index(drop=True)


def _process_transit_workbook(workbook_path: Path) -> list[dict[str, object]]:
    df = pd.read_excel(workbook_path, sheet_name="Invoice Detail")
    missing_columns = TRANSIT_REQUIRED_WORKBOOK_COLUMNS - set(df.columns)
    if missing_columns:
        raise Failure(f"Transit workbook is missing required columns: {sorted(missing_columns)}")

    df = df.copy()
    month_values = pd.to_datetime(df["Month of Use"], format="%m/%Y", errors="coerce")
    df = df.loc[month_values.notna()].copy()
    if df.empty:
        raise Failure("Transit workbook did not contain any valid `Month of Use` rows.")

    df["month"] = month_values.loc[month_values.notna()].dt.strftime("%Y-%m").to_numpy()

    monthly_rows: list[dict[str, object]] = []
    for month, month_df in df.groupby("month", sort=True):
        active_rider = int(month_df.loc[month_df["Total # of Taps"] > 0, "Customer Number"].nunique())
        unique_rider = int(month_df["Customer Number"].nunique())
        active_ratio = float(active_rider / unique_rider) if unique_rider else 0.0
        monthly_rows.append(
            {
                "local_bus": int(month_df["Local Bus"].fillna(0).sum()),
                "rapid_transit": int(month_df["Rapid Transit"].fillna(0).sum()),
                "total_taps": int(month_df["Total # of Taps"].fillna(0).sum()),
                "active_rider": active_rider,
                "unique_rider": unique_rider,
                "active_ratio": active_ratio,
                "month": month,
            }
        )

    return monthly_rows


def _find_month_link(page, month: str):
    links = page.locator("table tr td a")
    for candidate in _portal_month_candidates(month):
        locator = links.filter(has_text=candidate).first
        if locator.count() > 0:
            return locator
    return None


def _download_requested_workbooks(
    months: list[str],
    transit_browser: PlaywrightBrowserResource,
    username: str,
    password: str,
) -> list[DownloadedTransitWorkbook]:
    downloaded_files: list[DownloadedTransitWorkbook] = []
    with tempfile.TemporaryDirectory(prefix="mbta-transit-") as temp_dir:
        with transit_browser.browser_context() as browser_context:
            page = browser_context.new_page()
            page.goto(TRANSIT_DOWNLOAD_URL, wait_until="networkidle")
            page.fill(TRANSIT_LOGIN_USERNAME_SELECTOR, username)
            page.fill(TRANSIT_LOGIN_PASSWORD_SELECTOR, password)
            page.click(TRANSIT_LOGIN_BUTTON_SELECTOR)
            page.goto(TRANSIT_BILLING_HISTORY_URL, wait_until="networkidle")
            page.wait_for_selector(TRANSIT_BILLING_TABLE_SELECTOR, timeout=TRANSIT_PORTAL_TIMEOUT_MS)

            for month in months:
                month_link = _find_month_link(page, month)
                if month_link is None:
                    raise Failure(f"Transit billing month `{month}` was not found in the MBTA portal.")

                month_link.scroll_into_view_if_needed()
                month_link.wait_for(state="visible", timeout=TRANSIT_PORTAL_TIMEOUT_MS)
                with page.expect_download() as download_info:
                    month_link.click()
                    page.wait_for_selector(
                        TRANSIT_DOWNLOAD_BUTTON_SELECTOR,
                        timeout=TRANSIT_PORTAL_TIMEOUT_MS,
                    )
                    page.click(TRANSIT_DOWNLOAD_BUTTON_SELECTOR)

                download = download_info.value
                target_path = Path(temp_dir) / f"transit_{month}.xlsx"
                download.save_as(str(target_path))
                downloaded_files.append(DownloadedTransitWorkbook(month=month, file_path=target_path))
                page.go_back(wait_until="networkidle")
                page.wait_for_selector(TRANSIT_BILLING_TABLE_SELECTOR, timeout=TRANSIT_PORTAL_TIMEOUT_MS)

            persisted_files: list[DownloadedTransitWorkbook] = []
            for downloaded in downloaded_files:
                persisted = Path(tempfile.gettempdir()) / downloaded.file_path.name
                persisted.write_bytes(downloaded.file_path.read_bytes())
                persisted_files.append(DownloadedTransitWorkbook(month=downloaded.month, file_path=persisted))

    return persisted_files


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def historical_transit_monthly(dhub: ResourceParam[DataHubResource]):
    project_id = dhub.get_project_id(TRANSIT_PROJECT_NAME)
    logger.info(f"Found project id: {project_id}!")
    download_links = dhub.search_files_from_project(project_id, TRANSIT_HISTORY_SEARCH_TERM)
    if len(download_links) == 0:
        logger.error("No historical transit download links found!")
        return pd.DataFrame()

    download_link = download_links[0]
    source_filename = _extract_filename(download_link, f"{TRANSIT_HISTORY_SEARCH_TERM}.csv")
    history_df = pd.read_csv(download_link)
    return _normalize_transit_monthly_summary(
        history_df,
        source_type="historical",
        source_filename=source_filename,
        downloaded_at=datetime.now(),
    )


@asset(
    io_manager_key="postgres_replace",
    compute_kind="python",
    group_name="raw",
)
def newbatch_transit_monthly(
    config: TransitMonthlyConfig,
    transit_browser: ResourceParam[PlaywrightBrowserResource],
) -> Output[pd.DataFrame]:
    username = os.getenv("MBTA_TRANSIT_USERNAME")
    password = os.getenv("MBTA_TRANSIT_PASSWORD")
    if not username or not password:
        raise Failure("MBTA transit credentials are missing. Set `MBTA_TRANSIT_USERNAME` and `MBTA_TRANSIT_PASSWORD`.")
    requested_months = _month_range(config.start_month, config.end_month)
    logger.info(f"Downloading transit billing months: {requested_months}")
    downloaded_files = _download_requested_workbooks(requested_months, transit_browser, username, password)

    summaries = []
    for downloaded in downloaded_files:
        downloaded_at = datetime.now()
        rows = _process_transit_workbook(downloaded.file_path)
        for row in rows:
            row["source_type"] = "newbatch"
            row["source_filename"] = downloaded.file_path.name
            row["downloaded_at"] = downloaded_at
            row["statement_month"] = downloaded.month
            summaries.append(row)
        downloaded.file_path.unlink(missing_ok=True)

    if not summaries:
        raise Failure("No transit workbooks were downloaded for the requested months.")

    transit_df = pd.DataFrame(summaries)
    transit_df = transit_df.sort_values(["month", "statement_month", "downloaded_at"]).reset_index(drop=True)
    return Output(
        transit_df,
        metadata={
            "requested_months": MetadataValue.json(requested_months),
            "downloaded_month_count": transit_df["month"].nunique(),
            "statement_row_count": len(transit_df.index),
        },
    )


dhub_transit_monthly_sync = add_dhub_sync(
    asset_name="dhub_transit_monthly_sync",
    table_key=["staging", "stg_transit_monthly"],
    config={
        "filename": f"{TRANSIT_HISTORY_SEARCH_TERM}.csv",
        "project_name": TRANSIT_PROJECT_NAME,
        "description": "Merged historical and incremental MBTA transit monthly usage summary",
        "title": "MBTA Transit Monthly Summary",
    },
)
