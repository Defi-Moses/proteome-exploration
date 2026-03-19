# Design Decisions

## DD-001: Deployment Platform
We use Railway for the API and worker services in phase 1.

## DD-002: Package Manager
We use pnpm as the workspace package manager.

## DD-003: Data Backbone
Canonical storage is Parquet, queried with DuckDB.

## DD-004: Projection Adapter Strategy
Keep both projection paths:
- `project-fixture` for deterministic reproducible testing.
- `project-vcf` for real variant-backed projection.

## DD-005: Cross-Service Registry Publishing
API and worker can use separate Railway volumes, so worker publishes registry
to API via authenticated sync endpoint (`/internal/registry/sync`) instead of
assuming shared filesystem access.
