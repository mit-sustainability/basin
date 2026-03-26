# Quickstart: MITOS Platform Baseline

## Purpose

Use this guide to inspect and verify the current platform behavior that the baseline spec documents.

## Local Setup

1. Install Python 3.11.5 and create a virtual environment.
2. Run `pip install --upgrade pip`.
3. Run `make setup-dev` from the repository root.
4. `make setup-dev` now installs orchestrator dependencies and also runs `python -m playwright install chromium` so website scanning can run locally without extra manual steps.
## Verify the Orchestrator

1. Ensure environment variables for warehouse, Data Hub, and any required upstream systems are available.
2. Run `cd orchestrator && ./run_dagster_local.sh`.
3. Confirm Dagster loads assets, jobs, schedules, and sensors from `orchestrator/__init__.py`.
5. Run `make verify-website-content-health` to smoke-test a real `bucket_0` scan and confirm both outputs (`mit_sustainability_pages` and `mit_sustainability_links`) ingest non-empty website content.

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
