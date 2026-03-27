# Specs Workflow

This repository uses `specs/` for three different things:

- `001-platform-baseline/` documents the current brownfield platform behavior.
- `BACKLOG.md` is the active work queue.
- numbered feature spec folders are optional and should be used only for larger or riskier work.

## Ground Rules

- Do not overload `001-platform-baseline` for unrelated feature work.
- If code and baseline docs disagree, verify the code first, then correct the baseline docs in the same change.
- Every active or proposed work item should appear in `specs/BACKLOG.md`.
- Most new pipelines, assets, dbt models, resources, and utility functions do not need their own feature spec folder.

## Default Workflow For This Repo

### 1. Add or update the backlog item

Before creating a branch, record the work in `specs/BACKLOG.md` with:

- an ID
- a short title
- a status
- a type
- an expected scope
- a brief outcome statement
- optional branch or spec references

Recommended statuses:

- `proposed`
- `ready`
- `in_progress`
- `blocked`
- `done`
- `dropped`

### 2. Create or switch to the branch

For most work, create the branch normally and keep the backlog row updated.

Examples of work that usually stay in this lightweight path:

- a new Dagster asset or pipeline
- a new dbt model chain
- a resource enhancement
- a utility function extraction
- a targeted ops or docs change

### 3. Implement directly against the backlog item and baseline docs

During implementation:

- keep the backlog row accurate
- update baseline docs if the platform contract changed
- keep the change surface narrow
- state verification clearly in code review or commit context

### 4. Close the loop

When the work is done:

- mark the backlog item `done`
- note the branch if useful
- update `001-platform-baseline` if repository-wide behavior changed

## When To Create A Feature Spec Folder

Create a numbered feature spec folder only when the work is large enough that a written contract will reduce churn.

Typical triggers:

- work spans multiple asset domains
- work changes runtime contracts or warehouse boundaries
- work adds or materially changes an external integration
- work changes schedules, sensors, or deployment behavior
- work is ambiguous enough that multiple agents need a sharper written boundary

When that threshold is met, use the helper:

```bash
.specify/scripts/bash/create-new-feature.sh --json --short-name "short-name" "Feature description"
```

This creates:

- a new git branch like `002-short-name`
- a matching spec directory like `specs/002-short-name/`
- an initial `spec.md`

Use the generated branch and spec path instead of inventing your own numbering.

### If you created a feature spec folder, write the spec first

In `specs/<feature>/spec.md`:

- define the user stories
- define acceptance scenarios
- define functional requirements
- define measurable success criteria

Keep the spec focused on:

- what changes
- why it matters
- what must be true when done

Do not turn the spec into an implementation plan.

### If needed, create the implementation plan

After switching to the feature branch, run:

```bash
.specify/scripts/bash/setup-plan.sh --json
```

Then fill `specs/<feature>/plan.md` with:

- affected repo paths
- technical context
- constitution checks
- verification strategy
- any justified complexity

### Add supporting docs only when they reduce ambiguity

Common supporting files:

- `research.md`
- `data-model.md`
- `quickstart.md`
- `contracts/`
- `checklists/`

Use them when the feature has enough scope to justify them. Avoid empty ceremony.

## Deciding Whether To Update Baseline Or Create A New Spec

Update `001-platform-baseline` when:

- correcting reverse-engineered documentation
- documenting an existing behavior that was already in code
- refining repository-level contracts

Create a new feature spec when:

- adding a cross-domain behavior
- changing orchestration boundaries
- changing runtime contracts or external integrations
- changing schedules, deployment, or warehouse boundaries
- performing a scoped refactor with enough ambiguity that a written contract will prevent rework

## Minimum Done State For Routine Work

A routine item is ready to execute when:

- it has a backlog entry
- it names expected scope and outcome
- it has a branch when work starts
- any changed platform contract is reflected in baseline docs

## Minimum Done State For Spec-Driven Work

A spec-driven item is not fully specified until:

- it has a backlog entry
- it has a numbered branch and spec directory
- `spec.md` is concrete enough to test
- `plan.md` identifies touched paths and verification
- any changed platform contract is reflected in baseline docs
