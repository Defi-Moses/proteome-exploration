# Performance Gating Policy

## Required Inputs

- At least one `--perf-command`.
- At least one `--parity-command` for behavior-sensitive changes.

## Baseline Rules

- Baseline updates require explicit `--update-baseline`.
- Do not auto-update baselines from failing runs.

## Default Threshold Guidance

- Duration regression threshold: 10%
- RSS regression threshold: 15%

Use tighter thresholds for stable hot paths and wider thresholds for noisy integration paths.

## Evidence Rules

- Persist JSON report artifact.
- Include command list, durations, and memory values in review context.
- Include parity command outcomes in the same report.
