# Rightsizing Guidelines

## Inputs

- Hotpath profiler summaries (`summary.json`)
- Performance parity reports (`perf_parity_report.json`)

## Memory Policy

- Use observed max RSS with a configurable headroom (default 35%).
- Round recommendation up to nearest 2 GiB.

## CPU Policy

- Compare observed mean runtime against target duration.
- If runtime significantly above target, increase CPU tier.
- If runtime far below target and stable, consider reducing CPU tier.

## Safety Rules

- Do not reduce memory when OOM signatures are recent.
- Keep recommendations tied to explicit benchmark artifacts.
