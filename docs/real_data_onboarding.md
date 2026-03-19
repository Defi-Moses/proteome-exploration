# Real Data Onboarding

This project is implementation-complete on fixture data. To run on real data, complete source onboarding first.

## 1. Fill the source plan

Edit:

- `configs/sources/phase1_sources.yaml`

Replace each placeholder `download_url` value with an actual URL (or `file:///...` local path).
Entries with `enabled: false` are intentionally skipped by bootstrap until ready.

## 2. Dry-run the bootstrap

```bash
python3 scripts/bootstrap_real_data.py --config configs/sources/phase1_sources.yaml
```

Expected output includes `dry_run source=...`.

## 3. Execute downloads + manifests + lockfile

```bash
python3 scripts/bootstrap_real_data.py \
  --config configs/sources/phase1_sources.yaml \
  --execute
```

Outputs:

- `data/raw/<source_id>/<version>/...` (downloaded artifacts)
- `data/raw/manifests/<source_id>/<version>.json` (validated source manifests)
- `data/raw/manifests/manifest.lock.json` (immutable source lockfile)
- `data/raw/manifests/bootstrap_summary.json`

## 4. Run release build

Fixture release (for verification):

```bash
python3 scripts/release_phase1.py --label fixture-release-001
```

Release contract check:

```bash
python3 scripts/check_release_contract.py \
  --release-manifest data/releases/fixture-release-001/release_manifest.json
```

## 5. Validate VCF-backed projection path (no full pipeline run)

After `ccre_ref` is materialized and your pangenome VCF is available, run:

```bash
python3 scripts/run_phase1.py \
  project-vcf \
  --ccre-ref data/interim/smoke/ccre_ref.jsonl \
  --ccre-ref-format jsonl \
  --variants /absolute/path/to/hprc.vcf.gz \
  --output-dir data/interim/projection \
  --output-format jsonl
```

Optional controls:

- `--haplotypes /absolute/path/to/haplotype_ids.tsv` to restrict to a subset/order.
- `--max-variants 25000` to cap parsed variants for smoke/debug runs.

## 6. Configure worker for non-fixture ingest/validation

Set worker env vars before `pipeline_once|pipeline_loop`:

- `PANCCRE_PIPELINE_CCRE_BED=/data/raw/encode_ccre_v4/2026-01/GRCh38-cCREs.bed`
- `PANCCRE_PIPELINE_SOURCE_RELEASE=encode-v4-2026-01`
- `PANCCRE_PIPELINE_ASSAY_SOURCE=/data/raw/<assay_source>/<version>/<file>`
- `PANCCRE_PIPELINE_ASSAY_SOURCE_FORMAT=csv` (or `jsonl|parquet`)

## 7. Build a real assay source from Engreitz heldout benchmark

Use this when `PANCCRE_PIPELINE_ASSAY_SOURCE` is still fixture-backed.

Prerequisites:

- `bedtools` installed where normalization runs.
- Real cCRE BED already present at:
  `/data/raw/encode_ccre_v4/2026-01/GRCh38-cCREs.bed`
- Haplotype subset file present at:
  `/data/config/haplotypes/hprc_phase1_subset.tsv`

Download source benchmark:

```bash
mkdir -p /data/raw/engreitz_crispri_heldout5/2026-03-19
curl -fL \
  "https://github.com/EngreitzLab/CRISPR_comparison/raw/refs/heads/main/resources/crispr_data/EPCrisprBenchmark_combined_data.heldout_5_cell_types.GRCh38.tsv.gz" \
  -o /data/raw/engreitz_crispri_heldout5/2026-03-19/source.tsv.gz
```

Normalize into the pipeline assay contract:

```bash
python3 scripts/prepare_engreitz_assay_source.py \
  --source-tsv-gz /data/raw/engreitz_crispri_heldout5/2026-03-19/source.tsv.gz \
  --ccre-bed /data/raw/encode_ccre_v4/2026-01/GRCh38-cCREs.bed \
  --haplotypes /data/config/haplotypes/hprc_phase1_subset.tsv \
  --output-csv /data/raw/engreitz_crispri_heldout5/2026-03-19/panccre_assay_labels.csv
```

The script also emits:

- `panccre_assay_labels.rejects.csv` (rows dropped + reasons)
- `panccre_assay_labels.summary.json` (counts, rates, checksums)

Then set worker env:

- `PANCCRE_PIPELINE_ASSAY_SOURCE=/data/raw/engreitz_crispri_heldout5/2026-03-19/panccre_assay_labels.csv`
- `PANCCRE_PIPELINE_ASSAY_SOURCE_FORMAT=csv`

## Notes

- `project-fixture` remains the deterministic test adapter for reproducible fixture releases.
- `project-vcf` is available for real-data projection without changing downstream contracts (`hap_projection` schema stays fixed).
