---
name: pipeline-readiness-gate
description: Validate pipeline run readiness before executing worker or CLI pipeline flows. Use when preparing to run `pipeline_once` or `pipeline_loop`, changing worker env vars, switching projection mode, or troubleshooting configuration drift that causes late-stage failures.
---

# Pipeline Readiness Gate

## Overview

Run a strict preflight before any expensive pipeline execution. Fail fast on missing env vars, invalid mode combinations, invalid URLs, and missing filesystem dependencies.

## Quick Start

1. Run gate against current environment.
   - `python3 skills/pipeline-readiness-gate/scripts/check_pipeline_readiness.py`
2. Optionally load variables from an env file first.
   - `python3 skills/pipeline-readiness-gate/scripts/check_pipeline_readiness.py --env-file .env`
3. Enforce Railway linkage checks if run context requires it.
   - `python3 skills/pipeline-readiness-gate/scripts/check_pipeline_readiness.py --strict-railway-link`

## Workflow

1. Validate required env controls and cross-field constraints.
2. Validate file paths for projection, ingest, and assay inputs.
3. Validate publish-mode requirements (`local|api_sync|dual`).
4. Validate worker mode and writable output targets.
5. Stop execution if any error is present.

## Bundled Resources

- `scripts/check_pipeline_readiness.py`: executable preflight checker with text and JSON output modes.
- `references/panccre-readiness-contract.md`: env matrix and required combinations.

## Operating Rules

- Treat gate failures as blockers for expensive runs.
- Treat warnings as explicit risk acknowledgements.
- Record JSON output artifact for run reviews when needed.
