# Run Readiness

As of 2026-03-19, readiness depends on what "full run" means.

## 1) Infrastructure Full Run (end-to-end pipeline execution)

Status: **Ready**

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
