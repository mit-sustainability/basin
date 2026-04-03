from dagster import AssetSelection, define_asset_job

from orchestrator.assets.confluence_wiki import confluence_mitos_pages


confluence_wiki_snapshot_job = define_asset_job(
    name="confluence_wiki_snapshot_job",
    selection=AssetSelection.assets(confluence_mitos_pages),
)
