# Pan-cCRE: Pangenome-Aware Regulatory Element Registry

This repository contains the project specification for building a public, pangenome-aware registry of human regulatory elements that vary across haplotypes.

- Full spec: [PROJECT_SPEC.md](./PROJECT_SPEC.md)

## Summary

The project starts from ENCODE cCRE anchors and asks how each element behaves across human haplotypes: conserved, diverged, fractured, absent, duplicated, or locally replaced by inserted sequence. The core output is a reproducible registry and benchmark, not a new foundation model.

## Core Goals

- Build an auditable registry of polymorphic cCRE states across haplotypes.
- Prioritize altered loci by likely functional impact.
- Validate prioritization using held-out public assay resources (CRISPRi, MPRA, and related labels).
- Quantify whether scorer disagreement adds signal or reveals failure modes.

## Phase-1 Scope

- Human only, GRCh38-anchored, autosomal euchromatic regions.
- Immune/hematopoietic context only.
- ENCODE cCREs as anchors with local non-reference candidate discovery near altered loci.
- CPU-first pipeline with DuckDB + Parquet.
- Cheap feature extraction at scale; expensive model calls only on shortlist (~10^3–10^4).

## Pipeline Shape

1. Source manifests and checksums
2. Normalize reference tables
3. Project cCREs onto haplotypes
4. Call altered states
5. Discover local replacement candidates
6. Featurize and score (cheap first, expensive on shortlist)
7. Rank and calibrate
8. Evaluate with leakage-audited holdouts
9. Build registry files, API, and reports

## Key Design Rules

- Deterministic, versioned transforms with provenance on every derived row.
- Frozen manifests required for downstream builds.
- Sequence models are plug-ins, not hard-coded assumptions.
- No exhaustive expensive-model sweep in phase 1.
- Evaluation emphasizes publication/locus holdouts (not random row splits).

## Planned Outputs

- `polymorphic_ccre_registry.parquet`
- `replacement_candidates.parquet`
- `scorer_outputs.parquet`
- `validation_links.parquet`
- Schema docs and reproducible benchmark splits
- Minimal query API and static report figures/tables

## First Implementation Priorities

1. Implement manifest + source download manager.
2. Parse and materialize `ccre_ref` Parquet.
3. Build a chromosome-20 fixture (100 cCREs, 3 haplotypes).
4. Implement projection + `hap_projection` output.
5. Implement state caller + `ccre_state` output.
6. Join one assay source to create `validation_link`.
7. Build cheap features + baseline ranker.
8. Integrate AlphaGenome only after the above is working on fixture.
