---
name: "basin-dagster-asset-implementer"
description: "Use for implementing or modifying BASIN Dagster assets, jobs, resources, and adjacent dbt-facing pipeline code. Trigger for new pipelines, new assets, changed job selections, resource work, and most routine data-platform feature development."
compatibility: "BASIN repository with backlog-first workflow and baseline docs under specs/001-platform-baseline/"
metadata:
  author: "repo-local"
  source: "AGENTS.md and repository workflow"
---

# BASIN Dagster Asset Implementer

Use this skill for production work in this repository when the primary task is to add, modify, or connect:

- Dagster assets
- asset jobs
- resources or IO managers
- schedules or sensors
- dbt-facing pipeline boundaries

## Required Context

Read these first when the change is not trivial:

- `AGENTS.md`
- `specs/BACKLOG.md`
- `specs/001-platform-baseline/spec.md`
- `specs/001-platform-baseline/contracts/runtime-contracts.md`

## Working Style

- Prefer the smallest domain slice that solves the problem.
- Keep raw -> staging -> final boundaries explicit.
- Preserve existing Dagster composition patterns unless the task explicitly changes them.
- Treat external dependencies, credentials, and failure handling as part of the implementation, not an afterthought.

## Implementation Checklist

1. Identify the owned scope:
   - asset module
   - job selection
   - dbt model boundary
   - resource or utility dependency
2. Confirm whether the change is:
   - routine pipeline work, or
   - a cross-domain or contract change that should update baseline docs
3. Implement with explicit attention to:
   - asset keys and schema placement
   - idempotent write behavior
   - metadata and failure surfaces
   - compatibility with existing schedules, sensors, and downstream models
4. Add or update relevant unit tests.
5. If runtime contracts changed, update the matching baseline docs.

## Repo-Specific Expectations

- New pipeline work usually does not need a dedicated feature spec folder.
- New behavior should still map cleanly to a backlog item.
- If the change touches schedules, deployment, or external integrations, check whether the baseline docs need updates.

## Deliverable

A narrow, production-ready patch with:

- implementation changes
- relevant unit tests
- baseline doc updates when contracts changed
