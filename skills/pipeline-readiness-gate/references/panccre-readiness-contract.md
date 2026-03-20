# PANCCRE Readiness Contract

## Required Valid Enumerations

- `PANCCRE_WORKER_MODE`: `heartbeat|once|pipeline_once|pipeline_loop`
- `PANCCRE_PIPELINE_PROJECTION_MODE`: `fixture|vcf`
- `PANCCRE_REGISTRY_PUBLISH_MODE`: `local|api_sync|dual`
- `PANCCRE_PIPELINE_ASSAY_SOURCE_FORMAT`: `csv|jsonl|parquet`

## Conditional Requirements

If `PANCCRE_PIPELINE_PROJECTION_MODE=vcf`:
- Require `PANCCRE_PIPELINE_VARIANTS`.
- Require file at `PANCCRE_PIPELINE_VARIANTS` to exist.

If `PANCCRE_PIPELINE_HAPLOTYPES` is set:
- Require file to exist.

If `PANCCRE_PIPELINE_CCRE_BED` is set:
- Require file to exist.

If `PANCCRE_PIPELINE_ASSAY_SOURCE` is set:
- Require file to exist.
- Require valid `PANCCRE_PIPELINE_ASSAY_SOURCE_FORMAT`.

If `PANCCRE_REGISTRY_PUBLISH_MODE in {api_sync, dual}`:
- Require `PANCCRE_REGISTRY_SYNC_TOKEN`.
- Require either:
  - `PANCCRE_API_SYNC_URL` starts with `http://` or `https://`, or
  - `RAILWAY_SERVICE__PANCCRE_API_URL` is set.

## Filesystem Targets

Defaults should be writable by the running user:

- `PANCCRE_PIPELINE_OUTPUT_ROOT` (default `/data/runs`)
- `PANCCRE_PUBLISH_REGISTRY_DIR` (default `/data/registry`)
- `PANCCRE_REPORT_OUTPUT_ROOT` (default `/data/reports`)
- `PANCCRE_FREEZE_OUTPUT_ROOT` (default `/data/processed`)

## Railway Check Guidance

If this run depends on Railway context, verify:

- `railway` CLI exists
- Project is linked (`railway status` succeeds)

Treat Railway linkage as required only for Railway-driven workflows.
