# Worker Failure Signatures

## Memory / OOM

Indicators:
- `exit=-9`
- `Killed 137`
- `out of memory`
- `OOM`

Actions:
- Profile failing stage.
- Reduce working set or stream/chunk processing.
- Right-size Railway memory after measurement.

## Missing/Invalid Environment

Indicators:
- `PANCCRE_PIPELINE_VARIANTS must be set`
- `PANCCRE_PIPELINE_PROJECTION_MODE must be one of`
- `PANCCRE_REGISTRY_PUBLISH_MODE must be one of`
- `Unsupported PANCCRE_WORKER_MODE`

Actions:
- Correct env vars and rerun readiness gate.

## Missing File Path

Indicators:
- `FileNotFoundError`
- `No such file or directory`
- `registry publish candidate missing files`

Actions:
- Verify mounted volume/path and artifact stage outputs.
- Re-run from failing stage or earlier dependency stage.

## API Sync Failure

Indicators:
- `PANCCRE_REGISTRY_SYNC_TOKEN is required`
- `PANCCRE_API_SYNC_URL is not set`
- `registry API sync failed status=`

Actions:
- Validate token and endpoint wiring across worker/API.
- Validate publish mode and linked Railway domain assumptions.

## Unknown Failure

Indicators:
- Unmatched error pattern with non-zero exit.

Actions:
- Capture last 200 lines with stage context.
- Add new signature to this catalog after diagnosis.
