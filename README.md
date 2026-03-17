# Pan-cCRE Phase-1 Implementation TODOs

Canonical spec: [PROJECT_SPEC.md](./PROJECT_SPEC.md)

## Working Decisions

- Deployment platform: Railway (primary runtime for API + worker services)
- Package manager: pnpm (workspace-managed repo tasks)
- Data backbone: DuckDB + Parquet
- Scoring strategy: cheap features first, expensive scorers only on shortlist

## File Architecture (Scaffolded)

```text
.
├─ apps/
│  ├─ api/
│  └─ worker/
├─ configs/
│  ├─ contexts/
│  └─ scorers/
├─ data/
│  ├─ raw/
│  ├─ interim/
│  ├─ processed/
│  └─ registry/
├─ docs/
│  └─ schemas/
├─ notebooks/
├─ scripts/
├─ src/panccre/
│  ├─ cli/ manifests/ ingest/ normalize/ projection/ state_calling/
│  ├─ candidate_discovery/ features/ scorers/ ranking/ evaluation/
│  ├─ registry/ api/ reports/ utils/
├─ tests/
│  ├─ unit/
│  ├─ integration/
│  └─ golden/
├─ package.json
├─ pnpm-workspace.yaml
├─ railway.json
└─ PROJECT_SPEC.md
```

## Ordered Process (Execution Plan)

1. `TODO P0` Bootstrap and guardrails
- [ ] Freeze source manifest schema and validator.
- [ ] Implement deterministic run manifests for every pipeline command.
- [ ] Add schema validation checks that hard-fail on contract drift.

2. `TODO P1` cCRE ingestion and canonical tables
- [ ] Ingest ENCODE cCRE and materialize `ccre_ref` Parquet.
- [ ] Normalize coordinate conventions and provenance fields.
- [ ] Add unit tests for parser and interval normalization.

3. `TODO P2` Fixture-first projection
- [ ] Build chromosome-20 fixture (100 cCREs, 3 haplotypes).
- [ ] Implement projection into `hap_projection` with QC summaries.
- [ ] Implement state caller into `ccre_state` with config thresholds.

4. `TODO P3` Candidate discovery and cheap features
- [ ] Discover local replacement candidates for absent/fractured states.
- [ ] Materialize `replacement_candidate` and `feature_matrix`.
- [ ] Train/evaluate cheap baseline ranker on fixture.

5. `TODO P4` Validation and ranking
- [ ] Join one assay source into `validation_link`.
- [ ] Implement holdout generation and leakage auditing.
- [ ] Produce top-k metrics and baseline comparisons.

6. `TODO P5` Scorer fanout and disagreement
- [ ] Add open-model scorer adapter.
- [ ] Implement shortlist routing and AlphaGenome budget enforcement.
- [ ] Compute disagreement features and ablation results.

7. `TODO P6` Registry/API/deployment
- [ ] Build registry artifacts (`polymorphic_ccre_registry.parquet`, etc.).
- [ ] Expose minimal API endpoints (`/health`, `/ccre/{id}`, `/top_hits`, `/downloads`).
- [ ] Deploy API and worker services to Railway.

## Immediate Next TODOs (Start Here)

- [x] Implement `scripts/build_manifest.py` with manifest schema validation.
- [x] Add `ccre_ref` parser + writer in `src/panccre/ingest/`.
- [x] Commit a minimal chromosome-20 fixture in `tests/golden/`.
- [x] Wire the first end-to-end smoke command in `src/panccre/cli/`.
