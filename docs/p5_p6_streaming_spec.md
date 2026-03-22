# P5/P6 Memory Reduction Spec (Streaming/Chunked)

## 1) Goal

Reduce peak memory usage in late-stage pipeline steps so full-scale runs can complete on Railway without reducing output quality.

Target stages:
- `run-ablations`
- `build-registry`
- `build-phase1-report`

Code paths:
- `src/panccre/scorers/ablation.py`
- `src/panccre/registry/builder.py`
- `src/panccre/reports/phase1.py`

## 2) Scope And Guardrails

In scope:
1. Replace full-table materialization with streaming/chunked reads and writes in the three target stages.
2. Keep all existing output schemas and file contracts unchanged.
3. Keep report/regression semantics aligned with current fixtures and integration tests.

Out of scope (no scope creep):
1. No model/scorer logic changes.
2. No biological threshold retuning.
3. No API contract changes.
4. No infrastructure migration away from Railway.

## 3) Current Failure Modes

Observed behavior on real runs:
1. `run-ablations` OOM when loading full `feature_matrix` + full `disagreement_features`.
2. `build-registry` OOM when loading full `ccre_state`, `validation_link`, `scorer_outputs`, and `replacement_candidates` together.
3. `build-phase1-report` OOM when loading full registry/disagreement/scorer tables only to use filtered slices.

Root cause:
- Full pandas DataFrame materialization at late stages where upstream outputs are already large.

## 4) Success Criteria

Functional parity:
1. Same required output columns and file names for all three stages.
2. Same aggregate report semantics (`state_class_distribution`, `assay_enrichment`, `failure_mode_taxonomy`, top-k logic).
3. Same registry schema and row cardinality.
4. Same ablation summary shape and metric definitions.

Resource targets:
1. `run-ablations` peak memory reduced by filtering to validation-linked entities only.
2. `build-registry` peak memory reduced to bounded aggregates + row buffers.
3. `build-phase1-report` peak memory reduced to top-k heap + filtered subsets.

Estimated memory improvement (based on data-flow reduction):
1. `run-ablations`: ~70-95% lower peak memory (depends on validation coverage fraction).
2. `build-registry`: ~60-85% lower peak memory (removes multi-table full materialization).
3. `build-phase1-report`: ~80-95% lower peak memory (online aggregation + top-k heap).

## 5) Design Overview

### 5.1 Shared Pattern

Apply streaming readers for `jsonl/csv` and bounded-memory table handling:
1. Iterate row-by-row for large tables.
2. Keep only required aggregations or filtered rows in memory.
3. Write outputs incrementally in chunks.

### 5.2 `run-ablations`

Changes:
1. Add row iterator for `jsonl/csv/parquet` inputs.
2. Build `required_entity_ids` from `validation_link`.
3. Stream `feature_matrix` and `disagreement_features`, keeping only:
   - `entity_type == ref_state`
   - `entity_id` in `required_entity_ids`
4. Continue baseline/ablation math on reduced DataFrames.

Effect:
- Removes need to load full feature/disagreement tables for non-validated entities.

### 5.3 `build-registry`

Changes:
1. Replace full-input DataFrame reads with streaming loops.
2. Introduce `_TableRowWriter` for chunked output writing (`jsonl/csv/parquet`).
3. Stream `validation_link`:
   - write through to output
   - build compact evidence map by `entity_id`.
4. Stream `scorer_outputs`:
   - write through to output
   - build mean-delta aggregates by `entity_id` for ranking score.
5. Stream `replacement_candidates` as copy-through.
6. Stream `ccre_state` and emit registry rows directly, enriching each row from compact aggregate maps.

Effect:
- Eliminates simultaneous full-table materialization of all registry inputs.

### 5.4 `build-phase1-report`

Changes:
1. Keep `validation_link` load (small enough and needed for holdout context).
2. Stream full registry table once and compute online:
   - state distribution counts
   - failure taxonomy counts
   - validation enrichment counts
   - top-k heap for ranked loci
3. Stream `disagreement_features` and keep only validation-linked entities.
4. Stream `scorer_outputs` and keep only top-hit entity rows needed for case studies.
5. Render figures/tables/markdown from aggregated counters + filtered slices.

Effect:
- Large report inputs are transformed from full-memory DataFrames to bounded aggregations.

## 6) Quality And Validation Plan

1. Unit/integration tests:
   - `tests.unit.test_registry_builder`
   - `tests.unit.test_disagreement_ablation`
   - `tests.integration.test_p5_p6_pipeline_cli`
   - `tests.integration.test_report_bundle_cli`
   - `tests.integration.test_api_server`
2. Ensure output artifacts still exist and satisfy schema contracts.
3. Verify worker deployment remains healthy (`/health` + service status).
4. Confirm late-stage commands complete on real artifacts without OOM.

## 7) Rollout Plan

1. Implement streaming changes in code paths above.
2. Run local unit/integration suite.
3. Deploy worker service.
4. Execute targeted late-stage validation on real run artifacts.
5. Run one clean `pipeline_once` once assay inputs are fully wired.

Rollback:
1. Revert the three target modules if parity issues are found.
2. Keep prior fixture pipeline as minimal fallback path.

## 8) Deliverables

Code deliverables:
1. Streaming row iterator + filtered loading in `ablation.py`.
2. Streaming registry builder + chunked row writer in `registry/builder.py`.
3. Streaming/online aggregation path in `reports/phase1.py`.

Documentation deliverables:
1. This spec file.
2. Run-readiness status update in `docs/run_readiness.md`.
