# PANCCRE Railway Reference

## Service Roots

- API service root: `apps/api`
- Worker service root: `apps/worker`

## Service Alias Defaults

The helper script supports short aliases:

- `api` -> `${PANCCRE_RAILWAY_API_SERVICE:-panccre-api}`
- `worker` -> `${PANCCRE_RAILWAY_WORKER_SERVICE:-panccre-worker}`

Override these if your Railway service names differ:

```bash
export PANCCRE_RAILWAY_API_SERVICE="<actual-api-service-name>"
export PANCCRE_RAILWAY_WORKER_SERVICE="<actual-worker-service-name>"
```

## Canonical Deploy Commands

```bash
railway up --service <api-service-name> --detach apps/api
railway up --service <worker-service-name> --detach apps/worker
```

Use `--attach` only for interactive debugging sessions.

## Critical Runtime Variables

API:

- `PANCCRE_REGISTRY_DIR` (set to `/data/registry` when using a Railway volume)
- `PANCCRE_AUTO_SEED_REGISTRY` (optional, defaults to `1`)
- `PANCCRE_REGISTRY_SYNC_TOKEN` (required when worker publishes via API sync)

Worker (common production settings):

- `PANCCRE_WORKER_MODE=pipeline_loop`
- `PANCCRE_WORKER_INTERVAL_SEC=1800`
- `PANCCRE_PIPELINE_OUTPUT_ROOT=/data/runs`
- `PANCCRE_PUBLISH_REGISTRY_DIR=/data/registry`
- `PANCCRE_REGISTRY_PUBLISH_MODE=api_sync`
- `PANCCRE_REGISTRY_SYNC_TOKEN=<shared-secret>`
- `PANCCRE_FREEZE_OUTPUT_ROOT=/data/processed`
- `PANCCRE_REPORT_OUTPUT_ROOT=/data/reports`

## High-Signal Triage Commands

```bash
railway service status --all
railway service logs --service <service> --lines 200 --filter "@level:error OR traceback"
railway service logs --service <service> --build --lines 200
railway ssh --service <service>
```

## Volume Expectations

- Attach a Railway volume at `/data` for API and worker when running persistent pipeline flows.
- Keep registry under `/data/registry` so API and worker conventions remain consistent.
