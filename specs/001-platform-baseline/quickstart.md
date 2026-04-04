# Quickstart: MITOS Platform Baseline

## Purpose

Use this guide to inspect and verify the current platform behavior that the baseline spec documents.

## Local Setup

1. On Apple Silicon, ensure Rosetta is installed because the Oracle-backed local runtime uses an Intel (`x86_64`) Python process.
2. Install Python 3.11.5 and create the `oracle_client` pyenv virtual environment.
3. Open the repository root in VS Code so the workspace settings select the Intel terminal profile and `oracle_client` interpreter automatically.
4. Run `pip install --upgrade pip`.
5. Run `make setup-dev` from the repository root.
6. `make setup-dev` installs dbt, Dagster, the editable orchestrator package, and `playwright install chromium`.

## Verify the Orchestrator

1. Ensure environment variables for warehouse, Data Hub, and any required upstream systems are available.
2. Run `cd orchestrator && ./run_dagster_local.sh`.
3. Confirm Dagster loads assets, jobs, schedules, and sensors from `orchestrator/__init__.py`.
4. For the website content health domain, verify that Dagster exposes five partitions for `website_content_health_job` and that manual materialization can target any single bucket or all five buckets.
5. For the MBTA transit monthly domain, ensure `MBTA_TRANSIT_USERNAME` and `MBTA_TRANSIT_PASSWORD` are set before running the portal-backed backfill asset.

## Verify Tests

1. Run `make run-tests`.
2. Expect pytest coverage to focus on selected assets and resources rather than every domain module.

## Verify dbt Outputs

1. Run `make dbt_manifest` to regenerate the dbt manifest if needed.
2. Run `make serve-dbt-catalog` to generate and inspect the warehouse docs locally.

## Inspect Automation

1. Review `.github/workflows/run-test.yaml` for CI test execution.
2. Review `.github/workflows/build-and-deploy.yaml` for Dagster image build and EC2 deployment.
3. Review `.github/workflows/build-docs.yaml` for dbt docs publication.

## Use This Baseline for Future Work

When creating a new spec, start by naming:

- the affected asset domain
- the asset job or trigger path
- the external resource dependencies
- the warehouse layer and final outputs
- the tests or operational verification that will prove the change
