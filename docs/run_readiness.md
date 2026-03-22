# Run Readiness

As of 2026-03-19, readiness depends on what "full run" means.

## 1) Infrastructure Full Run (end-to-end pipeline execution)

Status: **Partially ready**

Definition:
- Worker executes the full pipeline chain through registry publish.
- API stays online and can serve `/health`, `/top_hits`, `/downloads`.
- Registry publish crosses service boundaries (`worker -> API`) via authenticated sync.

Current deployed configuration:
- Worker projection mode: `vcf`
- Worker cCRE ingest input:
  `/data/raw/encode_ccre_v4/2026-01/GRCh38-cCREs.bed`
- Worker VCF input:
  `/data/raw/pangenome_haplotype_alignments/1.1/hprc-v1.1-mc-grch38.vcfbub.a100k.wave.vcf.gz`
- Worker haplotype subset:
  `/data/config/haplotypes/hprc_phase1_subset.tsv`
- Registry publish mode: `api_sync`
- API/worker share `PANCCRE_REGISTRY_SYNC_TOKEN`

Run command (one-shot):
- Set worker variable `PANCCRE_WORKER_MODE=pipeline_once`
- Wait for completion in worker logs
- Set it back to `heartbeat` or `pipeline_loop` per operating preference

2026-03-20 update:
- `project-vcf` completes with real ENCODE + HPRC inputs after streaming/chunked rewrite.
- `call-states` was rewritten to stream `hap_projection` and now completes on full real inputs.
- `discover-candidates` was rewritten to stream `ccre_state` and now completes on full real inputs.
- `featurize` was rewritten to stream `ccre_state` + candidates and no longer materializes full feature matrices in memory.
- `build-validation-link` was rewritten to stream `ccre_state` entity IDs against assay entities and no longer OOMs.
- `evaluate-ranking` now streams `feature_matrix` and only keeps validation-linked entities in memory.
- `shortlist`, `score-fanout`, and `compute-disagreement` now run with streaming/chunked IO paths to avoid full-table materialization.

2026-03-22 update:
- `run-ablations` now streams `feature_matrix` + `disagreement_features` and only keeps validation-linked `ref_state` entities in memory.
- `build-registry` now streams all major inputs (`ccre_state`, `validation_link`, `scorer_outputs`, `replacement_candidates`) and writes outputs incrementally.
- `build-phase1-report` now streams registry/disagreement/scorer tables, computes aggregates online, and keeps only top-k rows plus validation-linked slices.
- Detailed design + guardrails are documented in [p5_p6_streaming_spec.md](./p5_p6_streaming_spec.md).

Operational caveat:
- `/data` can fill from historical pipeline run artifacts (`/data/runs`), which can surface as `Errno 28` ("No space left on device") even when memory is healthy.
- For validation runs, use `PANCCRE_PIPELINE_OUTPUT_ROOT=/tmp/runs` (ephemeral large disk) or prune old `/data/runs/*` artifacts before rerunning.

## 2) Biological Full Run (all major inputs are real and validated)

Status: **Partially ready**

What is already real:
- ENCODE cCRE V4 BED ingest input (worker configured via `PANCCRE_PIPELINE_CCRE_BED`).
- HPRC GRCh38 VCF input for projection.
- Cross-service registry publish path.

Remaining blockers:
1. Validation-link labels are still fixture-backed unless `PANCCRE_PIPELINE_ASSAY_SOURCE` is set.
2. Multiple optional external sources remain disabled in
   `configs/sources/phase1_sources.yaml` (intentional until access/quality is confirmed).
3. Projection/state thresholds are heuristic and still need biological calibration.

Assay blocker resolution path:

- Normalize Engreitz heldout benchmark with:
  `python3 scripts/prepare_engreitz_assay_source.py ...`
- Use the same cCRE BED as worker ingest:
  `/data/raw/encode_ccre_v4/2026-01/GRCh38-cCREs.bed`
- Use the same projection haplotype list:
  `/data/config/haplotypes/hprc_phase1_subset.tsv`
- Point worker to generated CSV via `PANCCRE_PIPELINE_ASSAY_SOURCE`.

## Practical next run settings

For a realistic non-fixture run, set these worker vars first:
- `PANCCRE_PIPELINE_CCRE_BED=/data/raw/encode_ccre_v4/2026-01/GRCh38-cCREs.bed`
- `PANCCRE_PIPELINE_SOURCE_RELEASE=encode-v4-2026-01`
- `PANCCRE_PIPELINE_ASSAY_SOURCE=/data/raw/<your_assay_source>/<version>/<file>`
- `PANCCRE_PIPELINE_ASSAY_SOURCE_FORMAT=csv` (or `jsonl|parquet`)

Then run with:
- `PANCCRE_WORKER_MODE=pipeline_once`
