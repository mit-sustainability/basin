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

## External Dependency Contract

- Credentials and endpoints are supplied by environment variables, not static files.
- Local Apple Silicon development that touches the Oracle-backed MIT warehouse resource runs through the Intel `oracle_client` Python environment so the Oracle client library can initialize reliably.
- Data Hub-based domains rely on successful project discovery and file search before reading source files.
- Confluence wiki extraction relies on `CONFLUENCE_BASE_URL`, `CONFLUENCE_PAT`, and `CONFLUENCE_SPACE_KEY`, plus successful pagination through the Confluence REST API.
- AWS-backed automation relies on S3 metadata reads, ECR image publishing, and EC2 restart commands succeeding in sequence.
- Selected heavy assets may be launched from the EC2-hosted Dagster instance into ad hoc ECS Fargate Spot tasks via Dagster Pipes. Those remote tasks must write owned Postgres tables directly and use the same environment-driven credentials contract as the EC2 service.
- `purchased_goods_invoice` is controlled directly by its Dagster `InvoiceConfig`. The asset config selects `execution_mode` (`local` or `ecs`) and `write_mode` (`append` or `replace`) for that run without relying on separate environment-variable defaults.
- `purchased_goods_invoice` no longer depends on a static Dagster Postgres IO manager write mode. Both local and ECS execution paths write the owned raw table through the same explicit Postgres contract so `append` versus `replace` stays aligned.
- Website content health monitoring relies on Playwright browser installation, MIT Sustainability sitemap availability, and outbound HTTPS requests for page and internal link checks.

## Documentation Contract

- dbt docs generation is a first-class published artifact.
- Any change that renames, removes, or materially changes final models should also update the relevant dbt documentation and, if needed, the reverse-engineered baseline docs.
