# Implementation Plan: Website Content Health Scan

**Branch**: `[website-content-health]` | **Date**: 2026-03-23 | **Spec**: [spec.md](./spec.md)

## Summary

Add a new website-content-health domain to the orchestrator. The slice uses the MIT Sustainability sitemap as the crawl frontier, assigns URLs into five static buckets, scrapes each page with Playwright, writes page and link outputs to PostgreSQL through the existing pandas IO manager, and schedules a weekly fan-out over all partitions.

## Touched Paths

- `orchestrator/assets/website_content_health.py`
- `orchestrator/partitions/website_content_health.py`
- `orchestrator/jobs/website_content_health.py`
- `orchestrator/schedules/mitos_warehouse.py`
- `orchestrator/__init__.py`
- `orchestrator/tests/assets/test_website_content_health.py`
- `orchestrator/tests/jobs/test_website_content_health.py`
- `orchestrator/requirements.in`
- `orchestrator/requirements.txt`
- `orchestrator/setup.py`
- `orchestrator/Dockerfile`
- `specs/BACKLOG.md`
- `specs/001-platform-baseline/`

## Technical Decisions

- Use static partitions named `bucket_0` through `bucket_4` so operators can run or backfill a full website scan deterministically.
- Use sitemap discovery to avoid baking navigation structure into code.
- Use a single multi-asset to avoid rendering each page twice when producing the two output tables.
- Append raw scan rows so repeated scans preserve history; include `scanned_at` and `scan_partition` in both outputs.
- Limit built-in health checks to direct HTTP status classification, with external URLs marked as unchecked rather than aggressively probed.

## Verification

- Add focused unit tests for sitemap parsing, partition selection, metadata extraction helpers, and the schedule fan-out behavior.
- Run targeted pytest modules for the new asset and schedule logic.
