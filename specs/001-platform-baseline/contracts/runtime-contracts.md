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
- Data Hub-based domains rely on successful project discovery and file search before reading source files.
- AWS-backed automation relies on S3 metadata reads, ECR image publishing, and EC2 restart commands succeeding in sequence.

## Documentation Contract

- dbt docs generation is a first-class published artifact.
- Any change that renames, removes, or materially changes final models should also update the relevant dbt documentation and, if needed, the reverse-engineered baseline docs.
