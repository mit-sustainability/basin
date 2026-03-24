from dagster import AssetSelection, define_asset_job

from orchestrator.assets.website_content_health import (
    mit_sustainability_content_health,
    mit_sustainability_links,
    mit_sustainability_unique_links,
)


website_content_health_job = define_asset_job(
    name="website_content_health_job",
    selection=AssetSelection.assets(
        mit_sustainability_content_health,
        mit_sustainability_unique_links,
        mit_sustainability_links,
    ),
)


website_content_health_link_check_job = define_asset_job(
    name="website_content_health_link_check_job",
    selection=AssetSelection.assets(
        mit_sustainability_unique_links,
        mit_sustainability_links,
    ),
)
