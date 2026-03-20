---
name: hotpath-profiler-lab
description: Benchmark and profile pipeline hot paths to identify the highest-return optimization targets. Use when optimizing runtime/memory, investigating stage-level slowdowns, or collecting reproducible before/after evidence for performance changes.
---

# Hotpath Profiler Lab

## Overview

Run controlled benchmark/profile sessions for high-cost commands and produce artifacts that drive optimization decisions.

## Quick Start

1. Profile a Python stage command.
   - `python3 skills/hotpath-profiler-lab/scripts/profile_hotpath.py --label call-states --command "python3 scripts/run_phase1.py call-states ..." --runs 3`
2. Profile non-Python commands without cProfile wrapping.
   - `python3 skills/hotpath-profiler-lab/scripts/profile_hotpath.py --label build-registry --command "python3 scripts/run_phase1.py build-registry ..." --skip-cprofile`
3. Review generated report.
   - `cat artifacts/hotpath_profiles/call-states/summary.json`

## Workflow

1. Choose one stage command and stable inputs.
2. Run 2-5 repetitions for noise reduction.
3. Capture wall time and RSS metrics.
4. Capture cProfile top functions for Python commands.
5. Compare before/after optimization deltas.

## Bundled Resources

- `scripts/profile_hotpath.py`: repeatable profiler and benchmark driver.
- `references/session-template.md`: run protocol for high-quality performance evidence.

## Operating Rules

- Keep input data and env constant across compared runs.
- Persist artifacts under versioned labels.
- Use performance-parity-gate before accepting optimization patches.
