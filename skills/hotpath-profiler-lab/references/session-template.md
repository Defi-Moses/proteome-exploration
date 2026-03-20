# Profiling Session Template

## Session Setup

- Stage label:
- Command under test:
- Input data snapshot:
- Environment variables:
- Repetition count:

## Required Outputs

- `summary.json`
- `summary.txt`
- per-run metrics (`duration_sec`, `max_rss_kb`)
- cProfile artifacts for Python commands

## Interpretation Guidance

- Prioritize high cumulative-time functions.
- Validate large speedups with parity checks.
- Validate memory improvements with repeated runs, not one-offs.
