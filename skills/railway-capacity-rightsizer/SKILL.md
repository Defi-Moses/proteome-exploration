---
name: railway-capacity-rightsizer
description: Recommend Railway CPU and memory settings from benchmark evidence instead of guesses. Use when worker stages are slow, memory pressure appears, or iteration speed needs improvement without sacrificing reliability.
---

# Railway Capacity Rightsizer

## Overview

Use this skill to translate measured runtime/memory artifacts into concrete Railway resource recommendations.

## Quick Start

1. Generate benchmark/profiling artifacts first.
   - `python3 skills/hotpath-profiler-lab/scripts/profile_hotpath.py ...`
2. Produce capacity recommendation.
   - `python3 skills/railway-capacity-rightsizer/scripts/recommend_capacity.py --benchmark-report artifacts/hotpath_profiles/call-states/summary.json --target-duration-sec 900`
3. Review suggested `cpu` and `memoryBytes` values before applying.

## Workflow

1. Load one or more benchmark report artifacts.
2. Estimate working RSS with configurable headroom.
3. Compare observed stage durations to target runtime.
4. Recommend worker CPU and memory settings with rationale.

## Bundled Resources

- `scripts/recommend_capacity.py`: evidence-driven rightsizing tool.
- `references/rightsizing-guidelines.md`: sizing heuristics and decision policy.

## Operating Rules

- Never resize based on a single noisy run if alternatives exist.
- Keep memory headroom explicit and conservative.
- Re-run performance-parity-gate after capacity changes.
