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
- Optional env vars:
  - `PANCCRE_WORKER_MODE` (`heartbeat` or `once`)
  - `PANCCRE_WORKER_INTERVAL_SEC` (heartbeat interval)

## Runtime notes

- Services call Python modules from `src` with `PYTHONPATH=../../src`.
- Service commands set `PYTHONPATH=/app/src` directly, so no extra path env var is required.
- `pnpm` remains the workspace package manager for service scripts.
- `apps/api/nixpacks.toml` and `apps/worker/nixpacks.toml` install both Node/pnpm and Python runtime dependencies.

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

No secrets are required for the current API/worker runtime.
