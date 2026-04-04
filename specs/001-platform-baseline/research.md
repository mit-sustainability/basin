# Research: MITOS Platform Baseline

## Top-Level Findings

### Repository purpose

- `README.md` describes BASIN as the MITOS data platform with two main code surfaces:
  - `orchestrator/` for Dagster assets, jobs, schedules, resources, and Docker packaging
  - `warehouse/` for dbt warehouse models and generated data catalog artifacts

### Orchestration runtime

- `orchestrator/__init__.py` assembles a single `Definitions` object.
- Asset modules are loaded from domain-focused files such as `food.py`, `waste.py`, `commuting.py`, `business_travel.py`, `purchased_goods.py`, `construction.py`, `engagement.py`, `campus_utility.py`, `ghg_inventory.py`, and `ghg_footprint.py`.
- The platform also includes a website content health domain that uses Playwright plus sitemap discovery to scan `https://sustainability.mit.edu` into raw page and hyperlink monitoring tables.
- Shared resources include:
  - PostgreSQL IO managers and connection resources
  - dbt CLI resource
  - MIT Data Hub resource
  - MIT warehouse resource
  - AWS S3 resource
  - AWS Lambda Pipes client

### Warehouse runtime

- `warehouse/dbt_project.yml` configures dbt models with:
  - `staging` schema and Dagster group metadata
  - `final` schema and Dagster group metadata
  - snapshots under the `snapshots` target schema
- `warehouse/models/` contains staging and final SQL models for travel, food, waste, utilities, purchased goods, commuting, attendance, and GHG inventory outputs.
- `warehouse/models/sources.yml` documents raw sources loaded by Dagster into the warehouse.

### Execution triggers

- `orchestrator/schedules/mitos_warehouse.py` defines:
  - a daily dbt materialization schedule across all dbt assets
  - a monthly business travel job schedule
  - a weekly website content health schedule that fans out five partitioned runs across the MIT Sustainability crawl
- `orchestrator/sensors/s3_bucket.py` defines an S3 file update sensor that triggers the GHG inventory job when `all-scopes-sync/quickbase_data.csv` changes in `mitos-landing-zone`.
- Local developer entry points:
  - `make setup-dev`
  - `make run-tests`
  - `orchestrator/run_dagster_local.sh`
  - `warehouse` dbt docs commands from `Makefile`

### Deployment and publishing

- `.github/workflows/build-and-deploy.yaml` builds the Dagster image, pushes it to ECR, copies `docker-compose.yaml` to EC2, pulls environment variables from AWS Secrets Manager, and restarts the service.
- The EC2-hosted deployment can selectively launch ad hoc ECS tasks for heavy assets because `dagster-aws` includes `PipesECSClient`; this is a better fit than `EcsRunLauncher` when only some workloads should leave EC2.
- `.github/workflows/build-docs.yaml` installs dbt, generates docs, customizes the generated site, and deploys it to GitHub Pages.
- Some Dagster assets also sync processed data back to MIT Data Hub via helper-generated assets such as the food sync path.

### Representative domain pattern

- A typical domain uses:
  - one or more Dagster raw assets that fetch or read data
  - optional Python staging assets for normalization or enrichment
  - dbt staging and final models
  - an asset job anchored to a staging dbt model and expanded upstream and downstream
- `orchestrator/jobs/food_job.py` is representative:
  - anchor on `stg_food_order`
  - include upstream and downstream dbt assets
  - include an explicitly named Python asset `dining_hall_swipes`

### Operational assumptions

- Environment variables provide credentials and runtime endpoints:
  - PostgreSQL warehouse access
  - MIT Data Hub API token
  - MIT warehouse credentials
  - energy management warehouse credentials
  - food categorization API endpoint
- The website content health scan also depends on Playwright browser binaries and outbound HTTPS access to `https://sustainability.mit.edu`.
- `orchestrator/constants.py` can parse the dbt project on load when `DAGSTER_DBT_PARSE_PROJECT_ON_LOAD` is set.
- PostgreSQL writes are managed through an IO manager that treats asset keys as schema and table names and supports `replace` or `append` semantics.

### Testing reality

- The repository has focused tests for:
  - construction asset behavior
  - PostgreSQL resource and IO manager behavior
  - Data Hub resource behavior
- There is not broad automated coverage across all domain assets, schedules, or sensors.

## Implications for Future Specs

- Future domain specs should anchor on existing job patterns rather than inventing new execution shapes without justification.
- Changes to raw assets need explicit external dependency and credential documentation.
- Changes to dbt models should be treated as contract changes because they feed both Dagster selections and published docs.
