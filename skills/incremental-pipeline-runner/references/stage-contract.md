# Incremental Stage Contract

## Stage Order

1. `ingest`
2. `project`
3. `call-states`
4. `discover-candidates`
5. `featurize`
6. `build-validation-link`
7. `build-holdouts`
8. `evaluate-ranking`
9. `shortlist`
10. `score-fanout`
11. `compute-disagreement`
12. `run-ablations`
13. `build-registry`
14. optional `freeze-evaluation`
15. optional `build-phase1-report`

## Core Assumptions

- `run_dir` already exists for mid-pipeline reruns.
- Artifact names follow `scripts/run_phase1.py` output conventions.
- Stage commands are executed with `PYTHONPATH=<repo>/src`.

## High-Risk Mistakes

- Starting at `project` in VCF mode without `PANCCRE_PIPELINE_VARIANTS`.
- Starting late stages when upstream artifacts are missing or stale.
- Mixing output formats across stage reruns.
