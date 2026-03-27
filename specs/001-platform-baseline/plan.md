# Implementation Plan: MITOS Platform Baseline

**Branch**: `[001-platform-baseline]` | **Date**: 2026-03-23 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-platform-baseline/spec.md`

## Summary

Reverse engineer the current BASIN repository into a stable baseline spec package. The output documents existing behavior rather than proposing a redesign: Dagster assets and jobs ingest and orchestrate sustainability datasets, dbt builds staging and final warehouse models, GitHub Actions deploy the Dagster service and publish dbt docs, and a small test suite covers selected resources and transformations.

## Technical Context

**Language/Version**: Python 3.11.5, SQL via dbt models
**Primary Dependencies**: Dagster, dagster-dbt, pandas, SQLAlchemy, boto3, requests, pytest, dbt
**Storage**: PostgreSQL warehouse with `raw`, `staging`, `final`, and snapshot-style layers; dbt artifacts in `warehouse/target`
**Testing**: `pytest` under `orchestrator/tests`, plus dbt docs generation as a structural check
**Target Platform**: Local developer machines, GitHub Actions, AWS ECR/EC2 deployment, AWS S3/Lambda integrations, MIT Data Hub
**Project Type**: Brownfield data platform and orchestration repository
**Performance Goals**: Reliable batch refreshes and safe reruns are more important than low-latency execution
**Constraints**: Environment-variable configuration, network-dependent upstream systems, sparse automated coverage outside selected assets and resources
**Scale/Scope**: Roughly a dozen asset modules, ten asset jobs, multiple warehouse models in staging and final layers, and three distinct automation triggers

## Constitution Check

- Pass: docs are derived from current repository behavior instead of desired future architecture.
- Pass: Dagster asset boundaries, dbt model contracts, runtime triggers, and external dependencies are named explicitly.
- Pass: change surface is documentation-only plus Spec Kit scaffold installation.
- Risk acknowledged: the repository contains behavior that is currently verified operationally more than by tests; baseline docs record that gap instead of masking it.

## Project Structure

### Documentation (this feature)

```text
specs/001-platform-baseline/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── runtime-contracts.md
└── checklists/
    └── requirements.md
```

### Source Code (repository root)

```text
.
├── .github/
│   └── workflows/
├── orchestrator/
│   ├── assets/
│   ├── jobs/
│   ├── resources/
│   ├── schedules/
│   ├── sensors/
│   └── tests/
├── warehouse/
│   ├── models/
│   ├── snapshots/
│   ├── tests/
│   └── target/
├── .specify/
└── .agents/
```

**Structure Decision**: Keep a single reverse-engineered baseline spec package for the whole platform. Domain-level details are summarized as repeatable patterns rather than split into one spec per asset module.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Single baseline spec spans multiple domains | The repository is already brownfield and the immediate need is orientation, not feature decomposition | Splitting into one spec per domain would add overhead before a platform-level baseline exists |
