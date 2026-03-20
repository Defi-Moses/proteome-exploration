---
name: worker-failure-triage
description: Diagnose PANCCRE worker failures from log output and map them to actionable root-cause buckets. Use when Railway worker runs fail, pipeline stages exit non-zero, sync publish fails, or memory/runtime incidents need immediate next-step commands.
---

# Worker Failure Triage

## Overview

Use this skill to collapse time-to-diagnosis after worker failures. Parse logs, identify the failing stage, classify the failure type, and emit next actions.

## Quick Start

1. Analyze a worker log file.
   - `python3 skills/worker-failure-triage/scripts/analyze_worker_failure.py --log-file /path/to/worker.log`
2. Analyze recent terminal output piped in.
   - `tail -n 400 /path/to/worker.log | python3 skills/worker-failure-triage/scripts/analyze_worker_failure.py`
3. Emit machine-readable report.
   - `python3 skills/worker-failure-triage/scripts/analyze_worker_failure.py --log-file /path/to/worker.log --json-out /tmp/triage.json`

## Workflow

1. Extract the last executed pipeline stage from `worker_pipeline_command` log lines.
2. Detect known failure signatures (OOM, missing env vars, path issues, publish/sync failures).
3. Produce confidence-weighted diagnosis and exact remediation commands.
4. Escalate unknown signatures with preserved evidence lines.

## Bundled Resources

- `scripts/analyze_worker_failure.py`: deterministic parser and classifier.
- `references/failure-signatures.md`: signature catalog and expected remediation patterns.

## Operating Rules

- Keep evidence lines in output so recommendations are auditable.
- Prefer smallest reversible next step first.
- Route recurring unknown failures into signature updates.
