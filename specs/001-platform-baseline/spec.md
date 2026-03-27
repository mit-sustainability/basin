# Feature Specification: MITOS Platform Baseline

**Feature Branch**: `[001-platform-baseline]`
**Created**: 2026-03-23
**Status**: Draft
**Input**: User description: "Reverse engineer the necessary spec docs for the existing BASIN code base using GitHub Spec Kit."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Trace End-to-End Platform Flow (Priority: P1)

A maintainer needs one place to understand how BASIN ingests sustainability data, transforms it, and publishes warehouse outputs without reading every module first.

**Why this priority**: This is the minimum useful baseline for any future change. Without it, every modification starts with rediscovery.

**Independent Test**: A maintainer can read this spec package and correctly answer where data comes from, what orchestrates it, where it is stored, and which workflows deploy or publish outputs.

**Acceptance Scenarios**:

1. **Given** a maintainer new to the repository, **When** they review the baseline spec package, **Then** they can identify the top-level subsystems: Dagster orchestration, dbt warehouse modeling, CI/CD deployment, and docs publishing.
2. **Given** a domain output such as food, waste, or travel reporting, **When** the maintainer uses the baseline docs, **Then** they can trace that output from raw ingestion through staging and final warehouse models.

---

### User Story 2 - Operate Scheduled and Manual Refresh Paths (Priority: P2)

An operator needs to know what runs automatically, what runs manually, and which external triggers or credentials are required.

**Why this priority**: The platform is only useful if operators can reason about refresh timing, failures, and rerun behavior.

**Independent Test**: A maintainer can use the baseline docs to identify existing schedules, sensors, local run commands, test commands, and deployment workflows.

**Acceptance Scenarios**:

1. **Given** an operator investigating why data is stale, **When** they read the baseline docs, **Then** they can identify the daily dbt schedule, monthly business travel schedule, and S3-driven GHG sensor.
2. **Given** a developer preparing a local run, **When** they read the baseline docs, **Then** they can identify the setup, Dagster dev, dbt docs, and pytest entry points already present in the repository.

---

### User Story 3 - Extend a Domain Pipeline Safely (Priority: P3)

A developer needs to know the expected extension pattern for adding or changing a domain pipeline without breaking warehouse contracts or orchestration behavior.

**Why this priority**: The repository contains multiple repeated domain slices; documenting the common pattern reduces accidental divergence.

**Independent Test**: A developer can use the baseline docs to describe the minimum files and contracts needed for a new or changed domain asset.

**Acceptance Scenarios**:

1. **Given** a developer adding a new domain asset, **When** they use the baseline docs, **Then** they can identify the required orchestration, warehouse, and test touch points.
2. **Given** a developer changing an existing domain asset, **When** they use the baseline docs, **Then** they can identify the affected asset job selection, dbt model boundary, external resource dependency, and verification steps.

---

### Edge Cases

- Source files exist in MIT Data Hub but do not match the expected naming convention or file format.
- Network-backed enrichment calls, such as the food categorization endpoint, time out or return malformed payloads.
- Environment variables required for warehouse, Data Hub, or AWS access are missing in local or CI execution.
- Asset code and dbt documentation drift apart because only a subset of domains currently has explicit tests.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The baseline spec package MUST identify the repository's primary subsystems and their responsibilities.
- **FR-002**: The baseline spec package MUST enumerate the current Dagster asset domains and the asset job pattern used to materialize them.
- **FR-003**: The baseline spec package MUST describe the warehouse layering strategy used by the repository, including `raw`, `staging`, `final`, and snapshot-oriented outputs where present.
- **FR-004**: The baseline spec package MUST document the existing execution triggers, including schedules, sensors, local developer commands, and CI/CD workflows.
- **FR-005**: The baseline spec package MUST name the external systems and environment-variable-backed credentials required by the current code paths.
- **FR-006**: The baseline spec package MUST document the current publication surfaces, including PostgreSQL warehouse tables, dbt docs output, and Data Hub synchronization paths where defined.
- **FR-007**: The baseline spec package MUST describe the standard extension path for adding or changing a domain pipeline across orchestration, transformation, and verification layers.
- **FR-008**: The baseline spec package MUST call out known repository risks or gaps that affect future spec accuracy, including sparse automated coverage and runtime assumptions that are enforced only by configuration.

### Key Entities *(include if feature involves data)*

- **Asset Domain**: A Dagster module such as food, waste, commuting, business travel, purchased goods, construction, engagement, campus utility, footprint, or GHG inventory that owns a slice of ingestion and transformation behavior.
- **Asset Job**: A Dagster-defined execution selection that materializes a domain slice by anchoring on one or more dbt models and selecting upstream or downstream assets.
- **Warehouse Layer**: A PostgreSQL or dbt-managed storage boundary, most commonly `raw`, `staging`, `final`, or snapshots, that marks data maturity and downstream expectations.
- **External Resource**: An authenticated system dependency such as MIT Data Hub, PostgreSQL, AWS S3, AWS Lambda Pipes, or MIT warehouse resources.
- **Publication Artifact**: A final table, dbt docs site, or synced Data Hub file intended for operational or analytical consumption.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A maintainer can identify the repository's four primary operating surfaces, orchestration, transformation, deployment, and documentation, from this spec package alone.
- **SC-002**: Every current asset job in `orchestrator/jobs/` is mapped to its domain purpose or anchor selection in the baseline docs.
- **SC-003**: The baseline docs list the current automatic triggers and manual run entry points without leaving unresolved placeholders.
- **SC-004**: A developer can derive the minimum change surface for a new or modified domain pipeline from this spec package without re-reading unrelated modules first.
