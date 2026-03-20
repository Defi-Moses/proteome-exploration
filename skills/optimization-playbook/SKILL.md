---
name: optimization-playbook
description: Standardize optimization work so speed/memory improvements maintain correctness and review quality. Use when planning optimization tasks, preparing optimization PRs, or validating that performance claims include baseline and parity evidence.
---

# Optimization Playbook

## Overview

Use this skill to keep optimization work disciplined: baseline first, targeted change second, parity and benchmark evidence always.

## Quick Start

1. Create an optimization brief artifact.
   - `python3 skills/optimization-playbook/scripts/create_optimization_brief.py --title "call-states memory reduction" --output docs/optimization/call-states-brief.md`
2. Fill baseline, hypothesis, and acceptance criteria before code changes.
3. Execute hotpath profiling + parity gate runs and attach artifacts to the brief.

## Workflow

1. Baseline: capture current runtime/memory and behavior evidence.
2. Hypothesis: describe expected speedup and risk boundary.
3. Change set: keep scope constrained to selected hot path.
4. Validation: run parity gate and benchmark/profile tools.
5. Decision: accept/reject based on evidence, not intuition.

## Bundled Resources

- `scripts/create_optimization_brief.py`: generate a structured optimization brief template.
- `references/patterns-and-anti-patterns.md`: approved optimization patterns and failure-prone anti-patterns.

## Operating Rules

- Do not merge optimization changes without baseline + after metrics.
- Do not trade correctness for speed without explicit product decision.
- Keep optimization scope narrow and measurable.
