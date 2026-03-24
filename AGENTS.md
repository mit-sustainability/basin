# AGENTS.md

## Working Rule

Decompose problems from first principles. Prefer concise, clean solutions over broad rewrites.

## Required Context

Before making non-trivial or cross-cutting changes, read the repository baseline specs:

- `.specify/memory/constitution.md`
- `specs/001-platform-baseline/spec.md`
- `specs/001-platform-baseline/plan.md`
- `specs/001-platform-baseline/research.md`
- `specs/001-platform-baseline/data-model.md`
- `specs/001-platform-baseline/contracts/runtime-contracts.md`
- `specs/001-platform-baseline/quickstart.md`

These files are the current reverse-engineered source of truth for:

- platform boundaries
- Dagster and dbt runtime contracts
- external dependencies
- deployment and docs workflows
- expected extension paths for new domain pipelines

## Backlog-First Workflow

- Treat `specs/BACKLOG.md` as the active work queue.
- Each backlog item should be a small, independently ownable unit of work such as a pipeline, asset, model chain, resource, utility, ops change, or docs change.
- Use one agent per backlog item when parallelizing work.
- Keep backlog status current when work is added, started, blocked, or completed.

## How To Use The Specs

- Treat `specs/001-platform-baseline/` as the default context for repository-level reasoning.
- If code and specs disagree, verify the code path first, then update the relevant baseline doc as part of the same change.
- Update the matching file under `specs/001-platform-baseline/` when you change orchestration, dbt model boundaries, runtime contracts, triggers, external integrations, or other repository-level behavior.
- Do not create a feature-specific spec folder by default.
- Create a feature-specific spec folder under `specs/` only when the work is cross-domain, changes runtime contracts, adds a new external integration, materially changes schedules or deployment, or is ambiguous enough that written design will prevent churn.
- Follow `specs/README.md` for the operating workflow.

## Repository Expectations

- Preserve the current Dagster composition model in `orchestrator/__init__.py` unless a spec explicitly changes it.
- Preserve raw -> staging -> final warehouse boundaries unless the change is intentional and documented.
- Keep external credentials and endpoints environment-variable driven.
- Favor minimal, verifiable changes.
- Prefer backlog item updates and baseline doc maintenance over generating ceremony-heavy feature specs for small pipeline work.

## Preferred Personas

Use these repo-local skills when the task matches:

- `.agents/skills/basin-dagster-asset-implementer/` for implementing or modifying Dagster assets, jobs, resources, and adjacent dbt-facing pipeline code.
- `.agents/skills/basin-code-reviewer/` for review passes focused on typing, docstrings, code quality, contract drift, and maintainability.
- `.agents/skills/basin-test-coverage-guardian/` for adding or improving unit tests whenever new behavior is introduced or existing behavior changes.

New feature work should normally involve both implementation and test-coverage review, even when the change is small.

## Spec-Kit Scripts

The `.specify/scripts/bash/` directory contains the shell automation that powers the spec-driven development workflow used by this repository. The `speckit-*` skills in `.agents/skills/` call these scripts internally. You do not need to invoke them directly unless you are bootstrapping a new feature branch outside of a skill.

| Script | Purpose |
|--------|---------|
| `create-new-feature.sh` | Creates a numbered git branch (`###-short-name`) and a matching spec directory under `specs/`, then copies the spec template. Run once at the start of spec-driven work. |
| `setup-plan.sh` | Copies the plan template into the current feature's spec directory. Run after `create-new-feature.sh` when a feature needs a written implementation plan. |
| `check-prerequisites.sh` | Validates that required spec artifacts (`spec.md`, `plan.md`, optionally `tasks.md`) exist before proceeding to the next workflow phase. Used internally by the speckit skills as a gate. |
| `update-agent-context.sh` | Parses `plan.md` and injects project metadata (language, framework, storage) into agent instruction files (`AGENTS.md`, `CLAUDE.md`, `.github/agents/copilot-instructions.md`, etc.) so AI assistants stay in sync with the current feature context. |
| `common.sh` | Shared helper functions (branch detection, path resolution, JSON escaping, template lookup) used by all other scripts. Not invoked directly. |

When to call these manually:
- Use `create-new-feature.sh` when starting a new spec-driven feature and no skill is available.
- Use `setup-plan.sh` when you have a `spec.md` but the feature directory is missing `plan.md`.
- Use `check-prerequisites.sh --paths-only` to inspect resolved feature paths without triggering validation.
- Use `update-agent-context.sh` after editing `plan.md` to propagate changes into agent instruction files.

The `specs/README.md` file documents the full workflow these scripts support.

## AWS Knowledge MCP

This repository ships a `.codex/config.toml` file used by the OpenAI Codex CLI (`codex` command). It registers the managed AWS Knowledge MCP endpoint as an available tool server:

- `https://knowledge-mcp.global.api.aws`

This file is **MCP server configuration only**. It has no relationship to the spec-kit workflow or the `.specify/` directory. Spec-kit state lives entirely in `specs/`, `.specify/`, and `.agents/skills/`.

Use the AWS Knowledge MCP for up-to-date AWS documentation, regional availability, and official AWS guidance when running inside the Codex CLI or any other client that loads `.codex/config.toml`.

Always consult the AWS Knowledge MCP server for AWS-specific questions before relying on memory alone.
