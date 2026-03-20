---
name: performance-parity-gate
description: Enforce speed and memory improvements without behavioral regressions by running benchmark commands and parity test commands against thresholds. Use before merge/deploy for optimization changes or when validating that performance work preserves output contracts.
---

# Performance Parity Gate

## Overview

Use this skill to make optimization changes safe. Measure command-level runtime and memory, run parity checks, compare against baseline, and fail if regression thresholds are exceeded.

## Quick Start

1. Run parity + perf checks with thresholds.
   - `python3 skills/performance-parity-gate/scripts/run_performance_parity_gate.py --perf-command "python3 -m unittest -q tests/unit/test_projection_vcf.py" --parity-command "python3 -m unittest -q tests/integration/test_projection_cli.py"`
2. Save baseline after an accepted run.
   - `python3 skills/performance-parity-gate/scripts/run_performance_parity_gate.py ... --baseline docs/perf/baseline.json --update-baseline`
3. Gate future runs against baseline.
   - `python3 skills/performance-parity-gate/scripts/run_performance_parity_gate.py ... --baseline docs/perf/baseline.json --max-duration-regression-pct 10`

## Workflow

1. Run parity commands first and fail on non-zero exit.
2. Run perf commands for configured repetitions.
3. Compare runtime/memory metrics against baseline thresholds.
4. Emit JSON artifact with pass/fail decision.

## Bundled Resources

- `scripts/run_performance_parity_gate.py`: command runner with baseline comparisons.
- `references/gating-policy.md`: policy defaults for thresholds and evidence handling.

## Operating Rules

- Never accept speed gains without parity pass.
- Keep regression thresholds explicit in command history.
- Update baseline only after approved quality review.
