# Project-Vcf Memory Reduction Spec (Streaming/Chunked)

## 1) Goal

Reduce peak memory usage of `project-vcf` enough to run full-scale cCRE + HPRC inputs in Railway without changing biological output quality.

Target command path:
- `python3 scripts/run_phase1.py project-vcf ...`
- implementation: `src/panccre/projection/vcf.py`

## 2) Scope And Guardrails

In scope:
1. `project-vcf` internals only (`src/panccre/projection/vcf.py`, related unit/integration tests).
2. Streaming/chunked output writing for `hap_projection`.
3. Memory-focused data structure changes in projection logic.
4. Preserve output contract (`HAP_PROJECTION_COLUMNS`) and QC semantics.

Out of scope (no scope creep):
1. No changes to `call-states` logic or downstream scoring/registry algorithms.
2. No changes to map-status heuristics (`_classify_variant`, `_status_metrics`) except bug fixes needed for parity.
3. No changes to assay normalization or holdout logic.
4. No infrastructure migration away from Railway in this work item.

## 3) Current Failure Mode

Observed runs were killed with `exit=-9` / `Killed 137` during `project-vcf`.

Root causes in current code (`src/panccre/projection/vcf.py`):
1. Full output materialization:
   - `build_vcf_hap_projection` builds `rows: list[dict]` for every `(ccre_id, haplotype_id)` pair.
   - At full scale this is ~2.35M cCRE x 12 haplotypes = ~28.2M rows in-memory before write.
2. DataFrame construction after list build:
   - `pd.DataFrame(rows, columns=HAP_PROJECTION_COLUMNS)` adds another large memory spike.
3. Duplicate cCRE table copies:
   - `ccre_by_chr = {chrom: frame.reset_index(...) for chrom, frame in ccre_ref.groupby(...)}` duplicates large table slices.
4. Per-variant overlap scan creates large temporary masks:
   - `overlaps = chr_frame[(chr_frame["start"] < ref_end) & (chr_frame["end"] > ref_start)]`.

Key insight: lowering `--max-variants` does **not** reduce final output cardinality, so memory still explodes when full matrix rows are materialized.

## 4) Success Criteria

Functional parity:
1. Same output schema and column order (`HAP_PROJECTION_COLUMNS`).
2. Same row count: `row_count == n_ccre * n_selected_haplotypes`.
3. Same deterministic row order as today:
   - outer loop by cCRE row order from `ccre_ref`
   - inner loop by selected haplotype order.
4. Same `map_status` assignment and metric derivation for each row.
5. Same QC summary semantics (`build_projection_qc_summary` equivalent values).

Resource targets:
1. Peak RSS for `project-vcf` reduced by at least 70% versus current baseline on full input.
2. Peak RSS stays below Railway memory cap (currently observed ~32 GB effective cap).
3. No unbounded growth with increasing cCRE count beyond sparse aggregate map.

## 5) Design Overview

### 5.1 Architectural Change

Replace full DataFrame materialization with a two-stage streaming pipeline:

1. Stage A: Parse VCF and accumulate **sparse non-exact events** only.
2. Stage B: Stream full `(ccre, haplotype)` projection rows directly to disk in chunks, using sparse aggregate lookups for non-exact rows and exact defaults otherwise.

This preserves full output quality while avoiding in-memory full table construction.

### 5.2 Data Structures

#### cCRE index (compact, immutable)

Load once from `ccre_ref`, but store projection-critical fields in compact arrays/lists:
1. `ccre_ids: list[str]`
2. `chrom_codes: list[int]` or `list[str]`
3. `starts: array[int32]`
4. `ends: array[int32]`

Per-chromosome index (no duplicated DataFrames):
1. `chrom_to_indices: dict[str, list[int]]` referencing global anchor indices.
2. For overlap scanning, per-chrom arrays of starts/ends by index.

#### Sparse aggregate map

Only store non-exact combinations:
1. key: packed integer `(anchor_idx * hap_count + hap_idx)` (or tuple of ints).
2. value: slotted struct with:
   - `map_status`
   - `event_count`
   - `delta_sum`
   - `alt_contig`
   - `has_inversion`

Rationale: exact rows dominate; sparse map is far smaller than full matrix.

### 5.3 Overlap Engine

Current boolean-mask scan is memory-inefficient. Replace with a bounded-memory overlap strategy:

Option selected for this scope:
1. Use per-chrom sorted anchors by `start`.
2. Use binary search (`bisect`) to get candidate window where `start < variant_end`.
3. Filter candidates by `end > variant_start`.

Notes:
1. This avoids allocating full-length boolean masks per variant.
2. Complexity improves substantially for sparse local overlaps.
3. Keep implementation dependency-free (no new interval-tree package).

### 5.4 Streaming Writer

Introduce internal row writer abstraction in `vcf.py`:

`ProjectionRowWriter` responsibilities:
1. Accept row dicts one-by-one.
2. Buffer up to `chunk_size` rows (default 100k).
3. Flush chunk to target format.
4. Maintain online QC accumulators (counts/min/max/means).
5. Close/finalize output and return row count + QC summary.

Format behavior:
1. `jsonl`: append newline-delimited JSON records per row/chunk.
2. `csv`: append with stable header once, then rows.
3. `parquet`:
   - If `pyarrow` available: write row groups incrementally.
   - If parquet engine unavailable: keep existing error behavior.

### 5.5 Execution Flow (New)

1. Read and validate cCRE reference (`read_ccre_ref`) once.
2. Build compact cCRE index and per-chrom lookup.
3. Parse VCF stream line-by-line:
   - parse genotype non-ref alleles
   - classify variant effect using existing logic
   - find overlapping anchor indices
   - update sparse aggregate map.
4. Open streaming writer.
5. For each anchor index in original cCRE order:
   - For each selected haplotype index in selected order:
     - Lookup sparse aggregate; if missing => exact defaults.
     - Compute derived metrics (`_status_metrics`, orientation rules, alt coords).
     - Write row to writer.
6. Finalize writer and emit QC summary JSON.
7. Return `ProjectionResult` with row count and artifact paths.

## 6) Backward Compatibility

External behavior preserved:
1. CLI arguments unchanged.
2. Output file names unchanged (`hap_projection.<fmt>`, `hap_projection_qc.json`).
3. Run manifest behavior unchanged in CLI.
4. `mapping_method` remains `vcf_projection_v1`.

Internal API note:
1. Keep `project_vcf_haplotypes(...)` public signature unchanged.
2. `build_vcf_hap_projection(...)` may remain as test helper (small inputs only) or be refactored to call streaming path then read-back for compatibility.

## 7) Quality And Validation Plan

### 7.1 Unit tests

Add/adjust tests in `tests/unit/test_projection_vcf.py`:
1. Parity test on fixture input:
   - old semantics vs new semantics for map-status counts and QC.
2. `max_variants` behavior preserved.
3. Deterministic ordering test on output rows.
4. Chunk boundary test:
   - force tiny `chunk_size` (e.g., 7 rows) and verify full output correctness.
5. Writer format tests for `jsonl` and `csv`.

### 7.2 Integration tests

Update `tests/integration/test_projection_cli.py`:
1. Ensure `project-vcf` still writes expected artifacts.
2. Validate run manifest row counts and paths unchanged.

### 7.3 Runtime validation (real-data smoke)

On worker (not full pipeline):
1. Run `project-vcf` with full cCRE + full haplotypes + bounded variants.
2. Capture:
   - runtime
   - peak RSS (using `/usr/bin/time -v` where available)
   - output row count and QC summary consistency.

Acceptance threshold:
1. No OOM kill under current Railway effective memory cap.
2. Output contract and QC checks pass.

## 8) Rollout Plan

1. Step 1: Refactor internals behind existing public API, keep behavior identical.
2. Step 2: Add parity + chunk tests.
3. Step 3: Run full unit/integration projection tests.
4. Step 4: Deploy worker.
5. Step 5: Execute one-off `project-vcf` verification on Railway.

Rollback:
1. Revert projection module to previous commit if parity/perf regressions occur.
2. Keep fixture projection path untouched as fallback for non-real-data runs.

## 9) Risks And Mitigations

Risk 1: sparse aggregate map still grows large on highly variant-dense input.
- Mitigation: packed-int keys + slotted value objects to minimize overhead.

Risk 2: Parquet incremental writing complexity.
- Mitigation: implement `jsonl/csv` streaming first (worker default uses `jsonl`), then parity-safe parquet row-group path.

Risk 3: ordering drift during refactor.
- Mitigation: explicit ordering tests and deterministic iteration contracts.

Risk 4: overlap algorithm regression.
- Mitigation: fixture parity tests plus targeted edge cases (boundary overlap, symbolic alleles, inversion orientation).

## 10) Deliverables

Code deliverables:
1. `src/panccre/projection/vcf.py` streaming/chunked implementation.
2. Unit/integration test updates for parity and chunk correctness.
3. Optional small helper module/class (if needed) within `src/panccre/projection/` only.

Documentation deliverables:
1. This spec file.
2. Brief note in `docs/run_readiness.md` once implementation is live (status update only, no design expansion).
