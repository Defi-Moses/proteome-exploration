# Design Decisions

## DD-001: Deployment Platform
We use Railway for the API and worker services in phase 1.

## DD-002: Package Manager
We use pnpm as the workspace package manager.

## DD-003: Data Backbone
Canonical storage is Parquet, queried with DuckDB.
