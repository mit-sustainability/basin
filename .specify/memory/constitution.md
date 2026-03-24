<!--
Sync Impact Report
- Initial reverse-engineered constitution for the existing BASIN codebase.
- No template overrides were required; current Spec Kit templates remain usable.
- Future specs should treat Dagster asset boundaries, dbt model contracts, and external data interfaces as first-class requirements.
-->
# BASIN Constitution

## Core Principles

### I. Code-Derived Truth
Specifications for this repository MUST be derived from observable repository behavior before they are used to drive changes. When docs and code disagree, the current code paths, deployment workflow, and runtime contracts take precedence until the divergence is resolved explicitly.

### II. Dagster and dbt Contract First
Every material behavior change MUST identify the affected Dagster assets, asset jobs, dbt models, schedules, sensors, and external resources. Changes are not complete until the spec states how data enters `raw`, how it is transformed into `staging` and `final`, and which execution path materializes the result.

### III. Idempotent Batch Operations
This platform is a batch data system, not an interactive application. Specs and implementations MUST preserve safe reruns, explicit write semantics (`replace` vs `append`), and predictable recovery from partial failures such as missing source files, failed upstream APIs, or warehouse write errors.

### IV. External Dependency Explicitness
All work MUST document the external systems it relies on: PostgreSQL warehouse, MIT Data Hub, dbt, AWS services, MIT warehouse resources, and any network APIs such as the food categorizer. Required environment variables, authentication assumptions, and failure modes must be named directly in the spec rather than implied.

### V. Minimal and Testable Change Surface
Changes SHOULD stay within the smallest domain slice that solves the problem. Backlog items and, when needed, specs MUST identify the minimum affected files, expected tests, and operational checks. Broad refactors, cross-domain rewrites, or new abstractions require explicit justification in the plan.

## Repository Constraints

- Primary stack: Python 3.11 Dagster orchestrator plus dbt warehouse models.
- Data movement path: external source or API -> Dagster asset -> PostgreSQL schema -> dbt model -> final warehouse output and optional Data Hub sync.
- Deployment path: GitHub Actions builds the Dagster image, pushes to ECR, and restarts an EC2-hosted service.
- Documentation path: dbt docs are generated from the `warehouse` project and published to GitHub Pages.
- Runtime configuration is environment-variable driven; secrets are not committed to the repository.

## Development Workflow

- This repository is backlog-first. `specs/BACKLOG.md` is the default work queue, and most routine data-platform work does not require a dedicated feature spec folder.
- Every backlog item must state a narrow outcome and expected scope so it can be owned independently.
- A dedicated feature spec is required only for cross-domain, contract-changing, schedule-changing, deployment-changing, integration-heavy, or otherwise ambiguous work.
- Every spec for a behavioral change must name the user-facing or operator-facing outcome, affected domain, and expected execution trigger.
- Every plan must list the concrete repository paths it will touch across `orchestrator/`, `warehouse/`, `.github/workflows/`, and docs when relevant.
- Every implementation must preserve or improve tests for changed resources, transformations, or orchestration logic. If no test exists yet, the plan must say what verification will substitute and why.
- Reverse-engineered baseline docs may summarize existing gaps, but new work must not normalize undocumented behavior as acceptable.

## Governance

This constitution governs all Spec Kit artifacts in this repository. Amendments require updating this file and any impacted spec, plan, or checklist expectations in the same change. Reviewers should reject specs that omit execution boundaries, external dependencies, or verification strategy.

**Version**: 1.1.0 | **Ratified**: 2026-03-23 | **Last Amended**: 2026-03-23
