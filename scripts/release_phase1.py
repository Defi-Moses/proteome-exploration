#!/usr/bin/env python3
"""Build a reproducible phase-1 release bundle on fixture data."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from panccre.cli.main import main as cli_main
from panccre.manifests.builder import compute_sha256


def _run_cli(args: list[str]) -> None:
    print(f"release_step command={' '.join(args)}")
    code = cli_main(args)
    if code != 0:
        raise RuntimeError(f"CLI command failed (exit={code}): {' '.join(args)}")


def _file_record(path: Path) -> dict[str, object]:
    return {
        "path": str(path.resolve()),
        "bytes": int(path.stat().st_size),
        "sha256": compute_sha256(path),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a complete phase-1 fixture release bundle")
    parser.add_argument("--label", required=True, help="Release label (immutable once created)")
    parser.add_argument("--output-root", default=str(ROOT / "data" / "releases"))
    parser.add_argument("--context-group", default="immune_hematopoietic")
    parser.add_argument("--source-release", default="fixture-2026-03")
    parser.add_argument("--shortlist-top", type=int, default=10000)
    parser.add_argument("--max-alphagenome-calls", type=int, default=10000)
    parser.add_argument("--case-study-count", type=int, default=3)
    parser.add_argument("--fixture-bed", default=str(ROOT / "tests" / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"))
    parser.add_argument("--haplotypes", default=str(ROOT / "tests" / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"))
    parser.add_argument("--assay-source", default=str(ROOT / "tests" / "golden" / "assays" / "validation_assay_fixture.tsv"))
    return parser


def build_release(args: argparse.Namespace) -> Path:
    label = str(args.label).strip()
    if not label:
        raise ValueError("Release label must not be empty")

    output_root = Path(args.output_root)
    release_dir = output_root / label
    if release_dir.exists():
        raise FileExistsError(f"Release output already exists: {release_dir}")
    release_dir.mkdir(parents=True, exist_ok=False)

    run_dir = release_dir / "run"
    smoke_dir = run_dir / "smoke"
    projection_dir = run_dir / "projection"
    state_dir = run_dir / "state"
    candidate_dir = run_dir / "candidates"
    feature_dir = run_dir / "features"
    validation_dir = run_dir / "validation"
    ranking_dir = run_dir / "ranking"
    scorer_dir = run_dir / "scorers"
    registry_dir = release_dir / "registry"
    processed_root = release_dir / "processed"
    report_dir = release_dir / "reports"

    _run_cli(
        [
            "smoke-ingest",
            "--fixture-bed",
            args.fixture_bed,
            "--output-dir",
            str(smoke_dir),
            "--context-group",
            args.context_group,
            "--source-release",
            args.source_release,
            "--output-format",
            "jsonl",
        ]
    )
    _run_cli(
        [
            "project-fixture",
            "--ccre-ref",
            str(smoke_dir / "ccre_ref.jsonl"),
            "--ccre-ref-format",
            "jsonl",
            "--haplotypes",
            args.haplotypes,
            "--output-dir",
            str(projection_dir),
            "--output-format",
            "jsonl",
        ]
    )
    _run_cli(
        [
            "call-states",
            "--hap-projection",
            str(projection_dir / "hap_projection.jsonl"),
            "--hap-projection-format",
            "jsonl",
            "--output-dir",
            str(state_dir),
            "--output-format",
            "jsonl",
        ]
    )
    _run_cli(
        [
            "discover-candidates",
            "--ccre-state",
            str(state_dir / "ccre_state.jsonl"),
            "--ccre-state-format",
            "jsonl",
            "--output-dir",
            str(candidate_dir),
            "--output-format",
            "jsonl",
        ]
    )
    _run_cli(
        [
            "featurize",
            "--ccre-state",
            str(state_dir / "ccre_state.jsonl"),
            "--ccre-state-format",
            "jsonl",
            "--replacement-candidates",
            str(candidate_dir / "replacement_candidates.jsonl"),
            "--replacement-candidates-format",
            "jsonl",
            "--output-dir",
            str(feature_dir),
            "--output-format",
            "jsonl",
        ]
    )
    _run_cli(
        [
            "build-validation-link",
            "--ccre-state",
            str(state_dir / "ccre_state.jsonl"),
            "--ccre-state-format",
            "jsonl",
            "--assay-source",
            args.assay_source,
            "--assay-source-format",
            "csv",
            "--output-dir",
            str(validation_dir),
            "--output-format",
            "jsonl",
        ]
    )
    _run_cli(
        [
            "build-holdouts",
            "--validation-link",
            str(validation_dir / "validation_link.jsonl"),
            "--validation-link-format",
            "jsonl",
            "--output-dir",
            str(validation_dir),
            "--output-format",
            "jsonl",
        ]
    )
    _run_cli(
        [
            "evaluate-ranking",
            "--feature-matrix",
            str(feature_dir / "feature_matrix.jsonl"),
            "--feature-matrix-format",
            "jsonl",
            "--publication-validation",
            str(validation_dir / "validation_link_publication.jsonl"),
            "--publication-validation-format",
            "jsonl",
            "--locus-validation",
            str(validation_dir / "validation_link_locus.jsonl"),
            "--locus-validation-format",
            "jsonl",
            "--output-dir",
            str(ranking_dir),
        ]
    )
    _run_cli(
        [
            "shortlist",
            "--feature-matrix",
            str(feature_dir / "feature_matrix.jsonl"),
            "--feature-matrix-format",
            "jsonl",
            "--top",
            str(max(args.shortlist_top, 1)),
            "--output-dir",
            str(scorer_dir),
            "--output-format",
            "jsonl",
        ]
    )
    _run_cli(
        [
            "score-fanout",
            "--feature-matrix",
            str(feature_dir / "feature_matrix.jsonl"),
            "--feature-matrix-format",
            "jsonl",
            "--shortlist",
            str(scorer_dir / "shortlist.jsonl"),
            "--shortlist-format",
            "jsonl",
            "--context-group",
            args.context_group,
            "--max-alphagenome-calls",
            str(max(args.max_alphagenome_calls, 1)),
            "--output-dir",
            str(scorer_dir),
            "--output-format",
            "jsonl",
        ]
    )
    _run_cli(
        [
            "compute-disagreement",
            "--scorer-outputs",
            str(scorer_dir / "scorer_outputs.jsonl"),
            "--scorer-outputs-format",
            "jsonl",
            "--output-dir",
            str(scorer_dir),
            "--output-format",
            "jsonl",
        ]
    )
    _run_cli(
        [
            "run-ablations",
            "--feature-matrix",
            str(feature_dir / "feature_matrix.jsonl"),
            "--feature-matrix-format",
            "jsonl",
            "--disagreement-features",
            str(scorer_dir / "disagreement_features.jsonl"),
            "--disagreement-features-format",
            "jsonl",
            "--publication-validation",
            str(validation_dir / "validation_link_publication.jsonl"),
            "--publication-validation-format",
            "jsonl",
            "--locus-validation",
            str(validation_dir / "validation_link_locus.jsonl"),
            "--locus-validation-format",
            "jsonl",
            "--output-dir",
            str(ranking_dir),
        ]
    )
    _run_cli(
        [
            "build-registry",
            "--ccre-state",
            str(state_dir / "ccre_state.jsonl"),
            "--ccre-state-format",
            "jsonl",
            "--replacement-candidates",
            str(candidate_dir / "replacement_candidates.jsonl"),
            "--replacement-candidates-format",
            "jsonl",
            "--scorer-outputs",
            str(scorer_dir / "scorer_outputs.jsonl"),
            "--scorer-outputs-format",
            "jsonl",
            "--validation-links",
            str(validation_dir / "validation_link.jsonl"),
            "--validation-links-format",
            "jsonl",
            "--output-dir",
            str(registry_dir),
            "--output-format",
            "csv",
            "--context-group",
            args.context_group,
        ]
    )
    _run_cli(
        [
            "freeze-evaluation",
            "--label",
            label,
            "--validation-dir",
            str(validation_dir),
            "--ranking-dir",
            str(ranking_dir),
            "--output-root",
            str(processed_root),
        ]
    )
    _run_cli(
        [
            "build-phase1-report",
            "--registry-dir",
            str(registry_dir),
            "--publication-ranking-report",
            str(ranking_dir / "ranking_publication_report.json"),
            "--locus-ranking-report",
            str(ranking_dir / "ranking_locus_report.json"),
            "--disagreement-features",
            str(scorer_dir / "disagreement_features.jsonl"),
            "--ablation-summary",
            str(ranking_dir / "disagreement_ablation_summary.json"),
            "--output-dir",
            str(report_dir),
            "--top-hits-k",
            "100",
            "--case-study-count",
            str(max(args.case_study_count, 1)),
        ]
    )

    freeze_manifest = processed_root / "frozen" / label / "freeze_manifest.json"
    bundle_manifest = report_dir / "bundle_manifest.json"
    report_markdown = report_dir / "report.md"
    top_hits = report_dir / "tables" / "top_100_ranked_loci.csv"

    key_files = [
        registry_dir / "polymorphic_ccre_registry.csv",
        registry_dir / "replacement_candidates.csv",
        registry_dir / "scorer_outputs.csv",
        registry_dir / "validation_links.csv",
        registry_dir / "registry_manifest.json",
        ranking_dir / "ranking_publication_report.json",
        ranking_dir / "ranking_locus_report.json",
        ranking_dir / "baseline_comparison.json",
        ranking_dir / "disagreement_ablation_summary.json",
        scorer_dir / "disagreement_features.jsonl",
        validation_dir / "holdout_summary.json",
        freeze_manifest,
        bundle_manifest,
        report_markdown,
        top_hits,
    ]

    missing = [str(path) for path in key_files if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Release build incomplete, missing files: {missing}")

    manifest_payload = {
        "label": label,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "release_dir": str(release_dir.resolve()),
        "context_group": args.context_group,
        "source_release": args.source_release,
        "inputs": {
            "fixture_bed": str(Path(args.fixture_bed).resolve()),
            "haplotypes": str(Path(args.haplotypes).resolve()),
            "assay_source": str(Path(args.assay_source).resolve()),
        },
        "artifacts": [_file_record(path) for path in key_files],
        "pointers": {
            "registry_dir": str(registry_dir.resolve()),
            "run_dir": str(run_dir.resolve()),
            "processed_root": str(processed_root.resolve()),
            "report_dir": str(report_dir.resolve()),
            "freeze_manifest": str(freeze_manifest.resolve()),
            "bundle_manifest": str(bundle_manifest.resolve()),
        },
    }

    release_manifest = release_dir / "release_manifest.json"
    release_manifest.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"release_manifest={release_manifest}")
    return release_manifest


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    build_release(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
