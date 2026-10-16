from dagster import AssetSelection, define_asset_job, in_process_executor

# In-process so the intermediate Documents/Bundle are read back by the same container.
knowledge_publish_job = define_asset_job(
    name="knowledge_publish_job",
    selection=AssetSelection.groups("knowledge"),
    executor_def=in_process_executor,
)
