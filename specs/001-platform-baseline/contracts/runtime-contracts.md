# Runtime Contracts

## Dagster Assembly Contract

- `orchestrator/__init__.py` is the composition root for assets, jobs, schedules, sensors, and resources.
- New platform behavior that should be runnable in Dagster must be reachable from this file, either directly or through imported modules.

## Asset Job Contract

- Jobs in `orchestrator/jobs/` generally follow one of two patterns:
  - anchor on a dbt staging model and select upstream plus downstream dependencies
  - directly define a selection string for a known asset key
- Changes to an existing domain should preserve the domain's job entry point unless the spec explicitly changes the operational unit.

## Warehouse Contract

- Raw assets persist tabular outputs through the PostgreSQL IO manager, which infers schema and table names from Dagster asset keys.
- dbt models consume those tables using source and model references and publish staging and final views or tables.
- Final model names and source documentation form part of the repository's published data contract.

## Emission Factor Contract

Defined in `specs/009-emission-factor-archive/`. Downstream pipelines get emission factors and inflation targets only from this contract and never hardcode a factor release or a target CPI year.

- **Archive.** `orchestrator/assets/emission_factors.py` holds a release registry (`RELEASES`). Each `raw.ef_archive_*` asset (`supply_chain`, `cornerstone_eeio`, `ghg_ef_hub`, `simap_food`) rebuilds its table from every registered release. A registered release that doesn't match exactly one DataHub file by display name fails the asset. Adding a release means one registry entry, plus a parser branch only if the layout changes.
- **Assignment.** `warehouse/seeds/emission_factor_assignment.csv` (`category, family, version, fy_start, fy_end`) is the only place that decides which release a category uses for a fiscal year. Existing fiscal years keep the release they were reported with; new fiscal years point at the latest release. dbt tests reject overlapping ranges and versions that aren't in the archive.
- **Contract model.** `staging.emission_factor_by_fiscal_year` has one row per `(category, fiscal_year, factor_key, factor_basis)` with `factor_value`, `unit`, `family`, `version`, `publisher`, `dollar_year` and `gwp_basis`. Open-ended assignments run through the next fiscal year. Join pattern:

  ```sql
  LEFT JOIN {{ ref('emission_factor_by_fiscal_year') }} AS ef
    ON ef.category = '<category>' AND ef.fiscal_year = t.fiscal_year
   AND ef.factor_key = t.<key> AND ef.factor_basis = 'spend'
  ```

- **Inflation.** `{{ inflate_to_dollar_year(amount, spend_year, dollar_year) }}` returns `amount * CPI(dollar_year) / CPI(spend_year)` from `raw.annual_cpi_index`.
  - `dollar_year` comes from the contract row.
  - `spend_year` is the transaction's calendar year, or the fiscal year only for already-annual sources.
  - A missing spend-year CPI falls back to the latest CPI; a missing target-year CPI yields NULL.
- **Legacy tables.** `emission_factor_useeio_v2`, `emission_factor_naics`, `commuting_emission_factors_EPA`, `waste_emission_factors_epa`, `food_emission_factor` and `annual_cpi_index` also live in `emission_factors.py`, with unchanged asset keys. Each legacy factor table is removed when its pipeline migrates to the contract.

## External Dependency Contract

- Credentials and endpoints are supplied by environment variables, not static files.
- Local Apple Silicon development that touches the Oracle-backed MIT warehouse resource runs through the Intel `oracle_client` Python environment so the Oracle client library can initialize reliably.
- Data Hub-based domains rely on successful project discovery and file search before reading source files.
- Confluence wiki extraction relies on `CONFLUENCE_BASE_URL`, `CONFLUENCE_PAT`, and `CONFLUENCE_SPACE_KEY`, plus successful pagination through the Confluence REST API.
- Knowledge bundle publishing (`knowledge_publish_job`, manual only) reads the Confluence root pages listed in `orchestrator/knowledge/config.py` (rendered `body.export_view`, walked via `/child/page`) and the public MkDocs repo `KNOWLEDGE_MKDOCS_REPO_URL` at `KNOWLEDGE_MKDOCS_REF`, then writes an immutable `s3://$KNOWLEDGE_S3_BUCKET/$KNOWLEDGE_S3_PREFIX/<bundle_version>.tar.gz` plus `ledger/<bundle_version>.json` using conditional puts (`IfNoneMatch=*`). It follows the Beacon bundle contract in `specs/008-knowledge-bundle-publisher/contracts/` and never updates Beacon deployment state. Optional env vars: `KNOWLEDGE_S3_BUCKET` (default `mitos-resources`), `KNOWLEDGE_S3_PREFIX` (`knowledgebase`), `KNOWLEDGE_MKDOCS_REPO_URL`, `KNOWLEDGE_MKDOCS_REF` (`main`), `KNOWLEDGE_MKDOCS_SITE_URL`. A `<prefix>/revoked.json` (`{"revoked": [...]}`) blocks republishing listed versions.
- AWS-backed automation relies on S3 metadata reads, ECR image publishing, and EC2 restart commands succeeding in sequence.
- Selected heavy assets may be launched from the EC2-hosted Dagster instance into ad hoc ECS Fargate Spot tasks via Dagster Pipes. Those remote tasks must write owned Postgres tables directly and use the same environment-driven credentials contract as the EC2 service.
- `purchased_goods_invoice` is controlled directly by its Dagster `InvoiceConfig`. The asset config selects `execution_mode` (`local` or `ecs`) and `write_mode` (`append` or `replace`) for that run without relying on separate environment-variable defaults.
- `purchased_goods_invoice` no longer depends on a static Dagster Postgres IO manager write mode. Both local and ECS execution paths write the owned raw table through the same explicit Postgres contract so `append` versus `replace` stays aligned.
- Website content health monitoring relies on Playwright browser installation, MIT Sustainability sitemap availability, and outbound HTTPS requests for page and internal link checks.
- MBTA transit monthly monitoring relies on Playwright browser installation, MBTA portal credentials supplied by environment variables, and successful Data Hub reads and syncs for historical and merged monthly tables.

## Documentation Contract

- dbt docs generation is a first-class published artifact.
- Any change that renames, removes, or materially changes final models should also update the relevant dbt documentation and, if needed, the reverse-engineered baseline docs.
