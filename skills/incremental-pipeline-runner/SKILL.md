---
name: incremental-pipeline-runner
description: Execute only the necessary subset of pipeline stages while preserving stage order and artifact dependencies. Use when iterating on one pipeline stage, reproducing a failure quickly, or rerunning downstream stages after a targeted change.
---

# Incremental Pipeline Runner

## Overview

Use this skill to shorten iteration loops by running a bounded stage window instead of full pipeline execution.

## Quick Start

1. Dry-run command plan from `project` to `build-registry`.
   - `python3 skills/incremental-pipeline-runner/scripts/run_incremental_pipeline.py --run-dir /data/runs/<tag> --start-stage project --end-stage build-registry`
2. Execute the same plan.
   - `python3 skills/incremental-pipeline-runner/scripts/run_incremental_pipeline.py --run-dir /data/runs/<tag> --start-stage project --end-stage build-registry --execute`
3. Run only late-stage rebuild.
   - `python3 skills/incremental-pipeline-runner/scripts/run_incremental_pipeline.py --run-dir /data/runs/<tag> --start-stage shortlist --end-stage build-registry --execute`

## Workflow

1. Select start and end stage.
2. Build deterministic command plan from existing artifact paths.
3. Validate required inputs for each stage.
4. Execute stages in-order or dry-run for review.

## Bundled Resources

- `scripts/run_incremental_pipeline.py`: stage-aware planner and executor.
- `references/stage-contract.md`: stage order and required artifact assumptions.

## Operating Rules

- Use dry-run first for unfamiliar stage windows.
- Avoid skipping dependency stages unless inputs already exist and are validated.
- Pair optimization iterations with performance-parity-gate.
