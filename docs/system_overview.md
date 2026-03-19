# System Overview

## Architecture

Services:
1. `@panccre/worker` (pipeline orchestrator)
2. `@panccre/api` (query/read API)

Storage:
1. Worker volume mounted at `/data`:
   - pipeline inputs (`/data/raw/...`)
   - run outputs (`/data/runs/...`)
2. API volume mounted at `/data`:
   - published registry (`/data/registry`)

Cross-service publish:
1. Worker builds registry artifacts in its own volume.
2. Worker packs artifacts into a `tar.gz` payload.
3. Worker POSTs payload to API internal endpoint:
   - `POST /internal/registry/sync`
   - authenticated via `X-PANCCRE-SYNC-TOKEN`
4. API validates payload and atomically publishes to `/data/registry`.

## Pipeline Flow

Primary CLI chain (worker-executed):
1. cCRE ingest:
   - fixture mode: `smoke-ingest`
   - real mode: `ingest-ccre` via `PANCCRE_PIPELINE_CCRE_BED`
2. Projection:
   - fixture: `project-fixture`
   - real VCF: `project-vcf`
3. `call-states`
4. `discover-candidates`
5. `featurize`
6. `build-validation-link`
7. `build-holdouts`
8. `evaluate-ranking`
9. `shortlist`
10. `score-fanout`
11. `compute-disagreement`
12. `run-ablations`
13. `build-registry`
14. Optional: `freeze-evaluation`
15. Optional: `build-phase1-report`
16. Publish registry (`local|api_sync|dual`)

## Key Runtime Controls

Projection:
- `PANCCRE_PIPELINE_PROJECTION_MODE=fixture|vcf`
- `PANCCRE_PIPELINE_VARIANTS`
- `PANCCRE_PIPELINE_HAPLOTYPES`
- `PANCCRE_PIPELINE_MAX_VARIANTS`

Ingest:
- `PANCCRE_PIPELINE_CCRE_BED` (optional override)
- `PANCCRE_PIPELINE_SOURCE_RELEASE`

Validation source:
- `PANCCRE_PIPELINE_ASSAY_SOURCE` (optional override)
- `PANCCRE_PIPELINE_ASSAY_SOURCE_FORMAT`

Publish:
- `PANCCRE_REGISTRY_PUBLISH_MODE=local|api_sync|dual`
- `PANCCRE_API_SYNC_URL`
- `PANCCRE_REGISTRY_SYNC_TOKEN`

Worker mode:
- `PANCCRE_WORKER_MODE=heartbeat|once|pipeline_once|pipeline_loop`
