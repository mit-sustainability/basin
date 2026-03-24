# Data Model: MITOS Platform Baseline

## Core Entities

### Asset Domain

- Represents a bounded sustainability data slice implemented in `orchestrator/assets/`.
- Examples include food, waste, business travel, purchased goods, commuting, construction, campus utility, engagement, and GHG inventory.
- Additional examples include website content health monitoring for MIT Sustainability pages and hyperlinks.
- Owns raw ingestion and any Python-side normalization that is not delegated to dbt.

### Asset Job

- Represents an executable Dagster selection defined in `orchestrator/jobs/`.
- Usually anchored on a staging dbt model and expanded to upstream and downstream dependencies.
- Defines the operational unit used by schedules, sensors, or manual runs.

### Warehouse Layer

- Represents a maturity boundary for stored data.
- Current layers include:
  - `raw`: direct ingests and reference tables
  - `staging`: cleaned and joined intermediate models
  - `final`: user-facing summaries and inventory outputs
  - `snapshots`: change-tracking state where dbt snapshots are used

### External Resource

- Represents a system that BASIN depends on at runtime.
- Current resources include PostgreSQL, MIT Data Hub, MIT warehouse access, AWS S3, AWS Lambda Pipes, and external HTTP APIs.

### Publication Artifact

- Represents an output consumed outside the immediate transform step.
- Examples include final warehouse tables, dbt docs on GitHub Pages, and Data Hub synchronized files.
- Raw monitoring tables such as website page snapshots and hyperlink health outputs are also publication artifacts for operator workflows.

## Relationships

- An **Asset Domain** contains multiple Dagster assets and may feed one or more dbt models.
- An **Asset Job** selects assets from one or more **Asset Domains** plus related dbt models.
- A **Warehouse Layer** stores outputs from both Python assets and dbt models.
- An **External Resource** is consumed by an **Asset Domain** during ingest, enrichment, or synchronization.
- A **Publication Artifact** is produced from a **Warehouse Layer** or from an enriched asset and exposed to downstream users or operators.

## Canonical Flow

1. External resource provides source data or trigger metadata.
2. Dagster raw asset ingests or pulls data into PostgreSQL `raw` or a Python processing step.
3. Optional Python staging asset normalizes, enriches, or categorizes records.
4. dbt staging and final models build warehouse-facing outputs.
5. Schedules, sensors, or manual job execution materialize the flow.
6. Final tables and docs become the primary published outputs, with selective sync back to Data Hub.
