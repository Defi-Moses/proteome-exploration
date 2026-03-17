# Railway Deployment

This repository uses two Railway services:

1. API service (`apps/api`)
2. Worker service (`apps/worker`)

## API service

- Root directory: `apps/api`
- Build config: `apps/api/railway.toml`
- Start command: `pnpm --filter @panccre/api start`
- Required env vars:
  - `PANCCRE_REGISTRY_DIR` (path to registry artifact directory)

## Worker service

- Root directory: `apps/worker`
- Build config: `apps/worker/railway.toml`
- Start command: `pnpm --filter @panccre/worker start`
- Optional env vars:
  - `PANCCRE_WORKER_MODE` (`heartbeat` or `once`)
  - `PANCCRE_WORKER_INTERVAL_SEC` (heartbeat interval)

## Runtime notes

- Services call Python modules from `src` with `PYTHONPATH=../../src`.
- `pnpm` remains the workspace package manager for service scripts.
