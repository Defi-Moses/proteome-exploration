#!/usr/bin/env python3
"""Normalize Engreitz CRISPR benchmark rows for PANCCRE validation-link ingestion."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from panccre.normalize import normalize_engreitz_assay_source


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare a contract-safe assay source from Engreitz CRISPR benchmark data")
    parser.add_argument("--source-tsv-gz", required=True, help="Path to EPCrisprBenchmark_*.GRCh38.tsv.gz")
    parser.add_argument(
        "--ccre-bed",
        default="/data/raw/encode_ccre_v4/2026-01/GRCh38-cCREs.bed",
        help="Path to the cCRE BED used by ingest-ccre in this deployment",
    )
    parser.add_argument(
        "--haplotypes",
        default="/data/config/haplotypes/hprc_phase1_subset.tsv",
        help="Path to haplotype subset file used by projection",
    )
    parser.add_argument(
        "--output-csv",
        required=True,
        help="Output CSV path for PANCCRE_PIPELINE_ASSAY_SOURCE",
    )
    parser.add_argument(
        "--rejects-csv",
        default=None,
        help="Optional rejects CSV path (defaults to <output stem>.rejects.csv)",
    )
    parser.add_argument(
        "--summary-json",
        default=None,
        help="Optional summary JSON path (defaults to <output stem>.summary.json)",
    )
    parser.add_argument("--bedtools-bin", default="bedtools")
    parser.add_argument("--min-overlap-bp", type=int, default=50)
    parser.add_argument("--assay-type", default="CRISPRi")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    result = normalize_engreitz_assay_source(
        source_path=args.source_tsv_gz,
        ccre_bed_path=args.ccre_bed,
        haplotypes_path=args.haplotypes,
        output_path=args.output_csv,
        rejects_path=args.rejects_csv,
        summary_path=args.summary_json,
        bedtools_bin=args.bedtools_bin,
        min_overlap_bp=args.min_overlap_bp,
        assay_type=args.assay_type,
    )

    print(
        "prepare_engreitz_assay_source_complete "
        f"source_rows={result.source_row_count} mapped_rows={result.mapped_row_count} "
        f"retained_rows={result.retained_entity_rows} output_rows={result.output_row_count} "
        f"reject_rows={result.reject_row_count} haplotypes={result.haplotype_count}"
    )
    print(f"output_csv={result.output_path}")
    print(f"rejects_csv={result.rejects_path}")
    print(f"summary_json={result.summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
