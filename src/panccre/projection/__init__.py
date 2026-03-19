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
from panccre.projection.vcf import build_vcf_hap_projection, project_vcf_haplotypes

__all__ = [
    "HAP_PROJECTION_COLUMNS",
    "ProjectionResult",
    "build_fixture_hap_projection",
    "build_projection_qc_summary",
    "build_vcf_hap_projection",
    "load_haplotype_ids",
    "project_fixture_haplotypes",
    "project_vcf_haplotypes",
    "validate_hap_projection_frame",
]
