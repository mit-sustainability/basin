---
name: "basin-code-reviewer"
description: "Use for BASIN code review passes. Focus on typing, docstrings, code quality, Dagster/dbt contract drift, maintainability, and whether the patch is scoped cleanly."
compatibility: "BASIN repository with Dagster, dbt, and backlog-first workflow"
metadata:
  author: "repo-local"
  source: "AGENTS.md and repository workflow"
---

# BASIN Code Reviewer

Use this skill when reviewing a patch, branch, or backlog item implementation.

## Review Priorities

1. Correctness and behavioral regressions
2. Typing quality and interface clarity
3. Docstrings where public or non-obvious behavior needs explanation
4. Code quality and maintainability
5. Contract drift against Dagster/dbt/runtime expectations
6. Test adequacy

## Required Context

Read as needed:

- `AGENTS.md`
- `specs/BACKLOG.md`
- `specs/001-platform-baseline/contracts/runtime-contracts.md`
- affected files under `orchestrator/`, `warehouse/`, `.github/workflows/`, or docs

## Review Checklist

- Are function signatures, return shapes, and resource interfaces typed clearly enough for maintenance?
- Are docstrings present where behavior, side effects, or failure modes are not obvious?
- Does the change preserve raw -> staging -> final boundaries?
- Does the change accidentally widen scope, add needless abstraction, or create cross-domain coupling?
- Are errors handled explicitly where external systems are involved?
- Did the patch update baseline docs if runtime contracts changed?
- Are the tests specific to the changed behavior instead of generic smoke coverage?

## Output Style

Lead with findings, ordered by severity.

Each finding should name:

- the file or area
- the concrete problem
- the likely consequence
- the missing test or missing doc update if relevant

If no findings exist, say so directly and note any residual testing gaps.
