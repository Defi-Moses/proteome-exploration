# Railway Deployment

This repository uses two Railway services:

1. API service (`apps/api`)
2. Worker service (`apps/worker`)

## API service

- Root directory: `apps/api`
- Build config: `apps/api/railway.toml`
- Start command: `PYTHONPATH=/app/src python3 -m uvicorn panccre.api.server:app --host 0.0.0.0 --port $PORT`
- Required env vars:
  - `PANCCRE_REGISTRY_DIR` (set to `/data/registry` when using Railway volume)
- Optional env vars:
  - `PANCCRE_AUTO_SEED_REGISTRY` (`1` default; creates placeholder registry files when missing)
- Railway-managed env vars:
  - `PORT` (injected by Railway; do not hardcode in service settings)

## Worker service

- Root directory: `apps/worker`
- Build config: `apps/worker/railway.toml`
- Start command: `PYTHONPATH=/app/src python3 -m panccre.worker.main`
- Worker build command installs pipeline runtime dependencies (`numpy`, `pandas`, `pyarrow`).
- Optional env vars:
  - `PANCCRE_WORKER_MODE` (`heartbeat`, `once`, `pipeline_once`, `pipeline_loop`)
  - `PANCCRE_WORKER_INTERVAL_SEC` (heartbeat interval)
  - `PANCCRE_PIPELINE_OUTPUT_ROOT` (default `/data/runs`)
  - `PANCCRE_PUBLISH_REGISTRY_DIR` (default `/data/registry`)
  - `PANCCRE_PIPELINE_CONTEXT` (default `immune_hematopoietic`)
  - `PANCCRE_PIPELINE_REGISTRY_FORMAT` (`csv` default)
  - `PANCCRE_PIPELINE_SHORTLIST_TOP` (default `10000`)
  - `PANCCRE_PIPELINE_PROJECTION_MODE` (`fixture` default; set `vcf` for variant-backed projection)
  - `PANCCRE_PIPELINE_VARIANTS` (required when projection mode is `vcf`; absolute path to VCF/VCF.GZ)
  - `PANCCRE_PIPELINE_HAPLOTYPES` (optional haplotype list path when projection mode is `vcf`)
  - `PANCCRE_PIPELINE_MAX_VARIANTS` (optional parse cap for smoke/debug runs in VCF projection mode)
  - `PANCCRE_REGISTRY_PUBLISH_MODE` (`local` default; `api_sync` recommended when API/worker use separate volumes)
  - `PANCCRE_API_SYNC_URL` (optional; defaults to `https://$RAILWAY_SERVICE__PANCCRE_API_URL/internal/registry/sync`)
  - `PANCCRE_REGISTRY_SYNC_TOKEN` (required when publish mode includes `api_sync`; must match API token)
  - `PANCCRE_MAX_ALPHAGENOME_CALLS` (optional scorer cap override)
  - `PANCCRE_FREEZE_EVALUATION` (`1` default)
  - `PANCCRE_FREEZE_LABEL` (optional explicit freeze label)
  - `PANCCRE_FREEZE_OUTPUT_ROOT` (default `/data/processed`)
  - `PANCCRE_BUILD_REPORT_BUNDLE` (`1` default)
  - `PANCCRE_REPORT_OUTPUT_ROOT` (default `/data/reports`)
  - `PANCCRE_REPORT_TOP_HITS_K` (default `100`)
  - `PANCCRE_REPORT_CASE_STUDY_COUNT` (default `3`)

## Runtime notes

- Services call Python modules from `src` with `PYTHONPATH=../../src`.
- Service commands set `PYTHONPATH=/app/src` directly, so no extra path env var is required.
- `pnpm` remains the workspace package manager for service scripts.
- `apps/api/nixpacks.toml` and `apps/worker/nixpacks.toml` install both Node/pnpm and Python runtime dependencies.
- Railway watch patterns include `/src/**`, `/scripts/**`, and `/configs/**` so shared pipeline code changes trigger service rebuilds.

## Railway storage setup

1. In Railway project settings, create a **Volume**.
2. Attach the volume to the **API service** and mount it at `/data`.
3. Set `PANCCRE_REGISTRY_DIR=/data/registry` on the API service.
4. Populate `/data/registry` with:
   - `polymorphic_ccre_registry.(jsonl|csv|parquet)`
   - `replacement_candidates.(jsonl|csv|parquet)`
   - `scorer_outputs.(jsonl|csv|parquet)`
   - `validation_links.(jsonl|csv|parquet)`
   - `registry_manifest.json` (recommended)

## Recommended worker settings for production pipeline runs

Set these on `@panccre/worker`:

- `PANCCRE_WORKER_MODE=pipeline_loop`
- `PANCCRE_WORKER_INTERVAL_SEC=1800`
- `PANCCRE_PIPELINE_OUTPUT_ROOT=/data/runs`
- `PANCCRE_PUBLISH_REGISTRY_DIR=/data/registry`
- `PANCCRE_PIPELINE_PROJECTION_MODE=fixture`
- `PANCCRE_REGISTRY_PUBLISH_MODE=api_sync`
- `PANCCRE_REGISTRY_SYNC_TOKEN=<shared-secret>`
- `PANCCRE_FREEZE_EVALUATION=1`
- `PANCCRE_FREEZE_OUTPUT_ROOT=/data/processed`
- `PANCCRE_BUILD_REPORT_BUNDLE=1`
- `PANCCRE_REPORT_OUTPUT_ROOT=/data/reports`

Set this on `@panccre/api`:

- `PANCCRE_REGISTRY_SYNC_TOKEN=<shared-secret>`

With `api_sync`, worker uploads a tarred registry payload to API internal endpoint
`/internal/registry/sync`; API atomically publishes it into its own mounted `/data/registry`.
