# Copilot Review Rules: Dagster & dbt (SQL Performance Focus)

## Dagster (Software-Defined Assets)
- **Asset Definitions:** Strictly use the `@asset` decorator. Ensure all assets have a `group_name` corresponding to their dbt layer (e.g., `raw`, `staging`, `final`).
- **I/O & Resources:** Flag any hardcoded credentials or connection strings. Insist on using `context.resources`.
- **Partitioning:** If an asset handles time-series data, check for `PartitionsDefinition`. If missing on a large dataset, suggest adding it to prevent memory issues.
- **Metadata:** Suggest adding `metadata={"dagster/column_schema": ...}` or row counts to assets to improve the Dagster UI observability.

## dbt & SQL Architecture
- **Layering Logic:** - `raw/`: Direct loads only. No transformations.
  - `staging/`: Basic renaming and type casting. Reference `source()`.
  - `final/`: Complex joins and business logic. Reference `ref()`.
- **SQL Performance (Critical):**
  - **No `SELECT *`:** Always flag `SELECT *` in any layer; it breaks schema evolution and hurts performance.
  - **Joins:** Ensure joins are on indexed/distributed keys. Suggest `DISTINCT` or `GROUP BY` if a join might cause a fan-out.
  - **CTEs:** Prefer CTEs over subqueries for readability and optimizer hints in certain warehouses (e.g., Snowflake/BigQuery).
  - **Partitions/Clusters:** If a table exceeds ~100GB, flag if a `partition_by` or `cluster_by` config is missing in the dbt `config` block.

## Testing & Quality (Grace Period)
- Do not block PRs for missing `unique` or `not_null` tests yet, but **suggest** them as "Future Improvements" if a primary key is identified.
