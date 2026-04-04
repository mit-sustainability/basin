from __future__ import annotations

from dagster_pipes import open_dagster_pipes

from orchestrator.assets.website_content_health import (
    BASE_URL,
    _link_refs_output_metadata,
    _pages_output_metadata,
    _scan_site,
)
from orchestrator.resources.playwright import PlaywrightBrowserResource
from orchestrator.resources.postgres_io_manager import (
    get_postgres_env_config,
    write_dataframe_to_table,
)


def main() -> None:
    with open_dagster_pipes() as pipes_context:
        pipes_context.log.info("Starting remote website content health scan.")
        browser_resource = PlaywrightBrowserResource(base_url=BASE_URL)
        pages_df, link_refs_df = _scan_site(browser_resource)

        postgres_config = get_postgres_env_config()
        write_dataframe_to_table(
            config=postgres_config,
            schema="raw",
            table="mit_sustainability_pages",
            obj=pages_df,
            write_method="replace",
        )
        write_dataframe_to_table(
            config=postgres_config,
            schema="raw",
            table="mit_sustainability_link_refs",
            obj=link_refs_df,
            write_method="replace",
        )

        pipes_context.report_custom_message(
            {
                "pages": _pages_output_metadata(pages_df),
                "link_refs": _link_refs_output_metadata(link_refs_df),
            }
        )
        pipes_context.log.info("Completed remote website content health scan.")


if __name__ == "__main__":
    main()
