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

## Task Definition Workflow

Dagster launches these remote entrypoints with `ecs:RunTask`; it does not create or update ECS services. The task definition must therefore stay aligned with the Dagster-side launcher configuration in `orchestrator/resources/ecs.py`.

At minimum, keep these values consistent:

- task definition family and revision exposed to Dagster as `ECS_TASK_DEFINITION`
- container definition name exposed to Dagster as `ECS_CONTAINER_NAME`
- a Linux image that contains the current `orchestrator/pipes/...` module
- runtime environment variables and secrets needed by the remote module itself

The Dagster process supplies the network configuration at launch time via:

- `ECS_CLUSTER`
- `ECS_TASK_DEFINITION`
- `ECS_CONTAINER_NAME`
- `ECS_SUBNET_IDS`
- `ECS_SECURITY_GROUP_IDS`
- `ECS_ASSIGN_PUBLIC_IP`
- `ECS_CAPACITY_PROVIDER_STRATEGY`

## Update A Task Definition With AWS CLI

When another asset needs ECS offload, the normal flow is to:

1. Build and push an image that contains the new `orchestrator.pipes.<asset>` module.
2. Export the current task definition JSON.
3. Edit the container image, environment variables, secrets, CPU, or memory as needed.
4. Register a new revision and point `ECS_TASK_DEFINITION` at that new revision.

Fetch the existing task definition:

```bash
aws ecs describe-task-definition \
  --task-definition basin-offload \
  --query taskDefinition > /tmp/basin-offload-task-definition.json
```

Edit the JSON into a new registration payload. Remove read-only fields such as:

- `taskDefinitionArn`
- `revision`
- `status`
- `requiresAttributes`
- `compatibilities`
- `registeredAt`
- `registeredBy`

Then register the new revision:

```bash
aws ecs register-task-definition \
  --cli-input-json file:///tmp/basin-offload-task-definition.json
```

If you prefer to patch the JSON from the CLI, `jq` is enough for the common case of changing only the image:

```bash
aws ecs describe-task-definition \
  --task-definition basin-offload \
  --query taskDefinition \
  --output json |
jq '
  del(
    .taskDefinitionArn,
    .revision,
    .status,
    .requiresAttributes,
    .compatibilities,
    .registeredAt,
    .registeredBy
  )
  | .containerDefinitions |= map(
      if .name == "run"
      then .image = "860551228838.dkr.ecr.us-east-1.amazonaws.com/mitos2023:basin_offload_test"
      else .
      end
    )
' > /tmp/basin-offload-task-definition.json

aws ecs register-task-definition \
  --cli-input-json file:///tmp/basin-offload-task-definition.json
```

After registration, update the Dagster launcher environment to the new revision, for example:

```bash
export ECS_TASK_DEFINITION="basin-offload:2"
export ECS_CONTAINER_NAME="run"
```

## Adding Another Offloaded Asset

For each new asset that should support ECS offload:

1. Add a thin module under `orchestrator/pipes/`.
2. Keep the shared business logic in `orchestrator/assets/...`.
3. Have the Dagster asset switch to `PipesECSClient.run(...)` when its config requests ECS execution.
4. Reuse the same task definition if the runtime shape is compatible, or register a new family/revision if the asset needs a different image, secrets, or task size.

Reusing one `basin-offload` family is fine when multiple assets can share:

- the same base image
- the same container name
- the same network model
- the same secret and environment shape

Create a separate task definition family only when an asset materially differs in:

- CPU or memory requirements
- container architecture
- IAM role requirements
- secret set
- image lineage
