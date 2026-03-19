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

## Notes

- `project-fixture` remains the deterministic test adapter for reproducible fixture releases.
- `project-vcf` is available for real-data projection without changing downstream contracts (`hap_projection` schema stays fixed).
