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

## Notes

- Current projection implementation is fixture-based deterministic projection (`project-fixture`), suitable for pipeline validation and reproducible release testing.
- Real haplotype projection adapters are a separate biological integration layer and should be added by replacing/augmenting projection inputs while preserving output contracts.
