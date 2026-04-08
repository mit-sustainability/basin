# ECS Pipes Entrypoints

This directory contains remote entrypoints for Dagster assets that may be dispatched from the main Dagster process into ECS via Dagster Pipes.

## When To Add A Module Here

Add a module under `orchestrator/pipes/` when an asset needs to support remote execution in ECS. The Dagster asset can then override the ECS container command with:

```python
["python", "-m", "orchestrator.pipes.some_asset"]
```

That module should stay thin. It should:

1. Call `open_dagster_pipes()`.
2. Read any injected extras or runtime config.
3. Call shared asset logic from `orchestrator/assets/...` rather than reimplementing business logic.
4. Perform the required side effect itself, such as writing to Postgres.
5. Report materialization metadata back to Dagster.

## Design Rule

Keep the business logic in the asset module and keep the `/pipes` module focused on remote bootstrap and reporting. If another asset needs ECS dispatch, add another small module here rather than embedding the remote workflow directly in the Dagster asset definition.
