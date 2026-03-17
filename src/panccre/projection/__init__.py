"""Projection utilities for cCRE haplotype state mapping."""

from panccre.projection.fixture import (
    HAP_PROJECTION_COLUMNS,
    ProjectionResult,
    build_fixture_hap_projection,
    build_projection_qc_summary,
    load_haplotype_ids,
    project_fixture_haplotypes,
    validate_hap_projection_frame,
)

__all__ = [
    "HAP_PROJECTION_COLUMNS",
    "ProjectionResult",
    "build_fixture_hap_projection",
    "build_projection_qc_summary",
    "load_haplotype_ids",
    "project_fixture_haplotypes",
    "validate_hap_projection_frame",
]
