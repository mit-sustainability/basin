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
- Optimize for human scanability and maintainability over local DRYness.
- Keep the main execution path easy to read without jumping across many helpers.

## Code Organization Rules

- Do not create multiple module-level helper functions unless they are clearly reused across separate call sites.
- Avoid function scattering: if logic belongs to one workflow or concept, keep it in one cohesive object.
- Prefer a small, focused class with methods over many hidden or private helper functions in the same file when the code represents one workflow.
- If a helper is used only once and does not remove meaningful noise, inline it.
- Do not extract a helper only to rename a few lines of logic.
- Shared logic should live in one obvious place, not be scattered across private functions.
- Keep module structure shallow and easy to scan.
- A reader should be able to understand the main path from the asset or public entrypoint without jumping through a deep call chain.

## Prefer Classes When

Use a class when the code represents one cohesive workflow with one or more of these traits:

- shared configuration
- shared dependencies or clients
- repeated internal state
- multiple related transformation or execution steps
- internal helper behavior that only makes sense within that workflow

Good examples in this repository include:

- extractors
- parsers
- API clients
- transformation workflows
- report builders
- loaders or uploaders

In these cases, prefer one focused class with a small public surface over many free functions.

## Class Constraints

- Do not create classes that are only namespaces for unrelated static methods.
- Keep each class focused on one responsibility.
- Prefer explicit constructor dependencies.
- Keep the public API small and obvious.
- Internal methods should be few and directly support the main execution flow.

## Helper Extraction Test

Before creating any new helper function or private method, apply this test:

1. Is it reused?
2. Does it hide noise rather than hide important logic?
3. Would inlining make the file easier to read?
4. Is the function name genuinely clearer than the body?

If the answer is mostly no, do not extract it.

## Avoid These Patterns

- many single-use helper functions in one module
- deep call chains for a simple asset workflow
- private helpers that merely rename a few lines
- utility dumping grounds
- fragmenting one logical operation across scattered free functions
- introducing new abstraction layers without clear reuse or conceptual value

## Dagster-Specific Expectations

- Keep asset definitions explicit and easy to trace.
- Make input/output boundaries obvious at the asset or workflow entrypoint.
- Prefer implementation shapes that make idempotency, writes, and failure surfaces easy to inspect.
- Keep side effects centralized and visible.
- Avoid hiding asset behavior behind too many thin wrappers.

## Implementation Checklist

1. Identify the owned scope:
   - asset module
   - job selection
   - dbt model boundary
   - resource or utility dependency

2. Confirm whether the change is:
   - routine pipeline work, or
   - a cross-domain or contract change that should update baseline docs

3. Design the implementation shape before coding:
   - what is the main public entrypoint
   - whether the workflow should stay inline, use a focused class, or extract a genuinely shared utility
   - how to keep the main path visible and easy to review

4. Implement with explicit attention to:
   - asset keys and schema placement
   - idempotent write behavior
   - metadata and failure surfaces
   - compatibility with existing schedules, sensors, and downstream models

5. Add or update relevant unit tests.

6. If runtime contracts changed, update the matching baseline docs.

## Repo-Specific Expectations

- New pipeline work usually does not need a dedicated feature spec folder.
- New behavior should still map cleanly to a backlog item.
- If the change touches schedules, deployment, or external integrations, check whether the baseline docs need updates.

## Deliverable

A narrow, production-ready patch with:

- implementation changes
- relevant unit tests
- baseline doc updates when contracts changed

## Definition of Done

The change is not done unless:

- the code follows existing repository structure and Dagster patterns
- the main execution flow is easy to understand from the entrypoint
- one-off helpers have been inlined or collapsed unless clearly justified
- shared logic is centralized in an obvious place
- new abstractions are minimal and justified by reuse or conceptual clarity
- tests cover the load-bearing behavior
- contracts and docs are updated when required
