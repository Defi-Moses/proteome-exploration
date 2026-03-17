"""CLI entrypoints for pan-ccre developer workflows."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from panccre.candidate_discovery import run_candidate_discovery
from panccre.evaluation import run_holdout_build, run_validation_link_build
from panccre.features import run_feature_build
from panccre.ingest import ingest_ccre_ref
from panccre.manifests import (
    build_manifest_entry,
    ensure_artifact_matches_checksum,
    fetch_source_artifact,
    load_manifest_file,
    upsert_manifest_lock_entry,
    write_manifest_file,
)
from panccre.projection import project_fixture_haplotypes
from panccre.ranking import run_ranking_evaluation
from panccre.state_calling import call_states_from_projection


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _add_fetch_source_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "fetch-source",
        help="Download source artifact and update manifest + lockfile",
    )
    parser.add_argument("--download-url", required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--license", required=True, dest="license_name")
    parser.add_argument("--genome-build", required=True)
    parser.add_argument("--parser-version", required=True)
    parser.add_argument("--name")
    parser.add_argument("--download-date")
    parser.add_argument("--format", dest="file_format")
    parser.add_argument("--notes")
    parser.add_argument("--filename")
    parser.add_argument("--raw-root", default=str(_repo_root() / "data" / "raw"))
    parser.add_argument("--manifest-root", default=str(_repo_root() / "data" / "raw" / "manifests"))
    parser.add_argument("--lock-file", default=str(_repo_root() / "data" / "raw" / "manifests" / "manifest.lock.json"))


def _add_ingest_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("ingest-ccre", help="Ingest BED-like cCRE source into canonical ccre_ref output")
    parser.add_argument("--input-bed", required=True)
    parser.add_argument("--manifest", help="Optional manifest to validate against input checksum")
    parser.add_argument("--context-group", default="immune_hematopoietic")
    parser.add_argument("--source-release", required=True)
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="parquet")
    parser.add_argument("--output-dir", default=str(_repo_root() / "data" / "processed" / "ccre_ref"))
    parser.add_argument("--output", help="Optional explicit output file path")


def _add_smoke_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("smoke-ingest", help="Run fixture manifest validation + cCRE ingest")
    parser.add_argument("--fixture-bed", default=str(_repo_root() / "tests" / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"))
    parser.add_argument("--manifest", help="Optional manifest JSON path to validate before ingest")
    parser.add_argument("--output-dir", default=str(_repo_root() / "data" / "interim" / "smoke"))
    parser.add_argument("--context-group", default="immune_hematopoietic")
    parser.add_argument("--source-release", default="fixture-2026-03")
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="jsonl")


def _add_project_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("project-fixture", help="Build fixture hap_projection from ccre_ref and haplotype IDs")
    parser.add_argument("--ccre-ref", default=str(_repo_root() / "data" / "interim" / "smoke" / "ccre_ref.jsonl"))
    parser.add_argument("--ccre-ref-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument("--haplotypes", default=str(_repo_root() / "tests" / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"))
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="jsonl")
    parser.add_argument("--output-dir", default=str(_repo_root() / "data" / "interim" / "projection"))


def _add_state_call_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("call-states", help="Call ccre_state rows from hap_projection")
    parser.add_argument("--hap-projection", default=str(_repo_root() / "data" / "interim" / "projection" / "hap_projection.jsonl"))
    parser.add_argument("--hap-projection-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="jsonl")
    parser.add_argument("--output-dir", default=str(_repo_root() / "data" / "processed" / "state"))


def _add_candidate_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("discover-candidates", help="Discover replacement candidates from ccre_state")
    parser.add_argument("--ccre-state", default=str(_repo_root() / "data" / "processed" / "state" / "ccre_state.jsonl"))
    parser.add_argument("--ccre-state-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="jsonl")
    parser.add_argument("--output-dir", default=str(_repo_root() / "data" / "processed" / "candidates"))


def _add_feature_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("featurize", help="Build feature_matrix from states and candidates")
    parser.add_argument("--ccre-state", default=str(_repo_root() / "data" / "processed" / "state" / "ccre_state.jsonl"))
    parser.add_argument("--ccre-state-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument("--replacement-candidates", default=str(_repo_root() / "data" / "processed" / "candidates" / "replacement_candidates.jsonl"))
    parser.add_argument("--replacement-candidates-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument("--feature-version", default="v1")
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="jsonl")
    parser.add_argument("--output-dir", default=str(_repo_root() / "data" / "processed" / "features"))


def _add_validation_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("build-validation-link", help="Join assay source into validation_link")
    parser.add_argument("--ccre-state", default=str(_repo_root() / "data" / "processed" / "state" / "ccre_state.jsonl"))
    parser.add_argument("--ccre-state-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument("--assay-source", default=str(_repo_root() / "tests" / "golden" / "assays" / "validation_assay_fixture.tsv"))
    parser.add_argument("--assay-source-format", choices=["parquet", "csv", "jsonl"], default="csv")
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="jsonl")
    parser.add_argument("--output-dir", default=str(_repo_root() / "data" / "processed" / "validation"))


def _add_holdout_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("build-holdouts", help="Generate publication/locus holdout splits and audit leakage")
    parser.add_argument("--validation-link", default=str(_repo_root() / "data" / "processed" / "validation" / "validation_link.jsonl"))
    parser.add_argument("--validation-link-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="jsonl")
    parser.add_argument("--output-dir", default=str(_repo_root() / "data" / "processed" / "validation"))


def _add_ranking_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("evaluate-ranking", help="Train/evaluate cheap baseline and output baseline comparisons")
    parser.add_argument("--feature-matrix", default=str(_repo_root() / "data" / "processed" / "features" / "feature_matrix.jsonl"))
    parser.add_argument("--feature-matrix-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument("--publication-validation", default=str(_repo_root() / "data" / "processed" / "validation" / "validation_link_publication.jsonl"))
    parser.add_argument("--publication-validation-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument("--locus-validation", default=str(_repo_root() / "data" / "processed" / "validation" / "validation_link_locus.jsonl"))
    parser.add_argument("--locus-validation-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument("--output-dir", default=str(_repo_root() / "data" / "processed" / "ranking"))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="pan-ccre CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_fetch_source_parser(subparsers)
    _add_ingest_parser(subparsers)
    _add_smoke_parser(subparsers)
    _add_project_parser(subparsers)
    _add_state_call_parser(subparsers)
    _add_candidate_parser(subparsers)
    _add_feature_parser(subparsers)
    _add_validation_parser(subparsers)
    _add_holdout_parser(subparsers)
    _add_ranking_parser(subparsers)
    return parser


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _artifact_extension(output_format: str) -> str:
    if output_format == "jsonl":
        return "jsonl"
    if output_format == "csv":
        return "csv"
    return "parquet"


def _write_run_manifest(command_name: str, output_dir: Path, inputs: dict[str, object], params: dict[str, object], outputs: dict[str, object], row_count: int | None = None) -> Path:
    payload: dict[str, object] = {
        "command": command_name,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": inputs,
        "params": params,
        "outputs": outputs,
    }
    if row_count is not None:
        payload["row_count"] = int(row_count)

    run_manifest_path = output_dir / "run_manifest.json"
    _write_json(run_manifest_path, payload)
    return run_manifest_path


def _handle_fetch_source(args: argparse.Namespace) -> int:
    download = fetch_source_artifact(
        download_url=args.download_url,
        raw_root=args.raw_root,
        source_id=args.source_id,
        version=args.version,
        filename=args.filename,
    )

    manifest = build_manifest_entry(
        file_path=download.artifact_path,
        source_id=args.source_id,
        version=args.version,
        download_url=args.download_url,
        license_name=args.license_name,
        genome_build=args.genome_build,
        parser_version=args.parser_version,
        download_date=args.download_date,
        name=args.name,
        file_format=args.file_format,
        notes=args.notes,
    )

    manifest_path = Path(args.manifest_root) / args.source_id / f"{args.version}.json"
    write_manifest_file(manifest, manifest_path)

    lock_path = upsert_manifest_lock_entry(
        lock_path=args.lock_file,
        manifest=manifest,
        artifact_path=download.artifact_path,
    )

    print(f"artifact_path={download.artifact_path}")
    print(f"artifact_reused={str(download.reused_existing).lower()}")
    print(f"manifest_written={manifest_path}")
    print(f"lockfile_updated={lock_path}")
    print(f"checksum={manifest.checksum}")
    return 0


def _run_ingest(*, input_bed: str, manifest_path: str | None, output_dir: str, context_group: str, source_release: str, output_format: str, explicit_output: str | None, command_name: str) -> int:
    bed_path = Path(input_bed)
    if not bed_path.exists():
        raise FileNotFoundError(f"Input BED not found: {bed_path}")

    validated_manifest_path: str | None = None
    if manifest_path:
        manifest = load_manifest_file(manifest_path)
        ensure_artifact_matches_checksum(bed_path, manifest.checksum)
        validated_manifest_path = str(Path(manifest_path).resolve())
        print(f"manifest_validated source_id={manifest.source_id} version={manifest.version}")

    if explicit_output:
        output_path = Path(explicit_output)
    else:
        extension = _artifact_extension(output_format)
        output_path = Path(output_dir) / f"ccre_ref.{extension}"

    result = ingest_ccre_ref(
        bed_path=bed_path,
        output_path=output_path,
        context_group=context_group,
        source_release=source_release,
        output_format=output_format,
    )

    out_dir = Path(output_dir)
    run_manifest_path = _write_run_manifest(
        command_name=command_name,
        output_dir=out_dir,
        inputs={"input_bed": str(bed_path.resolve()), "manifest": validated_manifest_path},
        params={"context_group": context_group, "source_release": source_release, "output_format": output_format},
        outputs={"ccre_ref": str(result.output_path.resolve())},
        row_count=result.row_count,
    )

    print(f"{command_name}_complete rows={result.row_count} output={result.output_path}")
    print(f"run_manifest={run_manifest_path}")
    return 0


def _handle_ingest_ccre(args: argparse.Namespace) -> int:
    return _run_ingest(
        input_bed=args.input_bed,
        manifest_path=args.manifest,
        output_dir=args.output_dir,
        context_group=args.context_group,
        source_release=args.source_release,
        output_format=args.output_format,
        explicit_output=args.output,
        command_name="ingest-ccre",
    )


def _handle_smoke_ingest(args: argparse.Namespace) -> int:
    return _run_ingest(
        input_bed=args.fixture_bed,
        manifest_path=args.manifest,
        output_dir=args.output_dir,
        context_group=args.context_group,
        source_release=args.source_release,
        output_format=args.output_format,
        explicit_output=None,
        command_name="smoke-ingest",
    )


def _handle_project_fixture(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    extension = _artifact_extension(args.output_format)

    output_path = output_dir / f"hap_projection.{extension}"
    qc_path = output_dir / "hap_projection_qc.json"

    result = project_fixture_haplotypes(
        ccre_ref_path=args.ccre_ref,
        haplotypes_path=args.haplotypes,
        output_path=output_path,
        qc_summary_path=qc_path,
        output_format=args.output_format,
        ccre_ref_format=args.ccre_ref_format,
    )

    run_manifest_path = _write_run_manifest(
        command_name="project-fixture",
        output_dir=output_dir,
        inputs={"ccre_ref": str(Path(args.ccre_ref).resolve()), "haplotypes": str(Path(args.haplotypes).resolve())},
        params={"ccre_ref_format": args.ccre_ref_format, "output_format": args.output_format},
        outputs={"hap_projection": str(result.output_path.resolve()), "qc_summary": str(result.qc_summary_path.resolve())},
        row_count=result.row_count,
    )

    print(f"project-fixture_complete rows={result.row_count} output={result.output_path}")
    print(f"qc_summary={result.qc_summary_path}")
    print(f"run_manifest={run_manifest_path}")
    return 0


def _handle_call_states(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    extension = _artifact_extension(args.output_format)

    output_path = output_dir / f"ccre_state.{extension}"
    qc_path = output_dir / "ccre_state_qc.json"

    result = call_states_from_projection(
        projection_path=args.hap_projection,
        output_path=output_path,
        qc_summary_path=qc_path,
        output_format=args.output_format,
        projection_format=args.hap_projection_format,
    )

    run_manifest_path = _write_run_manifest(
        command_name="call-states",
        output_dir=output_dir,
        inputs={"hap_projection": str(Path(args.hap_projection).resolve())},
        params={"hap_projection_format": args.hap_projection_format, "output_format": args.output_format},
        outputs={"ccre_state": str(result.output_path.resolve()), "qc_summary": str(result.qc_summary_path.resolve())},
        row_count=result.row_count,
    )

    print(f"call-states_complete rows={result.row_count} output={result.output_path}")
    print(f"qc_summary={result.qc_summary_path}")
    print(f"run_manifest={run_manifest_path}")
    return 0


def _handle_discover_candidates(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    extension = _artifact_extension(args.output_format)

    output_path = output_dir / f"replacement_candidates.{extension}"
    qc_path = output_dir / "replacement_candidates_qc.json"

    result = run_candidate_discovery(
        ccre_state_path=args.ccre_state,
        ccre_state_format=args.ccre_state_format,
        output_path=output_path,
        qc_summary_path=qc_path,
        output_format=args.output_format,
    )

    run_manifest_path = _write_run_manifest(
        command_name="discover-candidates",
        output_dir=output_dir,
        inputs={"ccre_state": str(Path(args.ccre_state).resolve())},
        params={"ccre_state_format": args.ccre_state_format, "output_format": args.output_format},
        outputs={"replacement_candidates": str(result.output_path.resolve()), "qc_summary": str(result.qc_summary_path.resolve())},
        row_count=result.row_count,
    )

    print(f"discover-candidates_complete rows={result.row_count} output={result.output_path}")
    print(f"qc_summary={result.qc_summary_path}")
    print(f"run_manifest={run_manifest_path}")
    return 0


def _handle_featurize(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    extension = _artifact_extension(args.output_format)
    output_path = output_dir / f"feature_matrix.{extension}"

    result = run_feature_build(
        ccre_state_path=args.ccre_state,
        ccre_state_format=args.ccre_state_format,
        replacement_candidate_path=args.replacement_candidates,
        replacement_candidate_format=args.replacement_candidates_format,
        output_path=output_path,
        output_format=args.output_format,
        feature_version=args.feature_version,
    )

    run_manifest_path = _write_run_manifest(
        command_name="featurize",
        output_dir=output_dir,
        inputs={"ccre_state": str(Path(args.ccre_state).resolve()), "replacement_candidates": str(Path(args.replacement_candidates).resolve())},
        params={
            "ccre_state_format": args.ccre_state_format,
            "replacement_candidates_format": args.replacement_candidates_format,
            "feature_version": args.feature_version,
            "output_format": args.output_format,
        },
        outputs={"feature_matrix": str(result.output_path.resolve())},
        row_count=result.row_count,
    )

    print(f"featurize_complete rows={result.row_count} output={result.output_path}")
    print(f"run_manifest={run_manifest_path}")
    return 0


def _handle_build_validation_link(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    extension = _artifact_extension(args.output_format)
    output_path = output_dir / f"validation_link.{extension}"

    result = run_validation_link_build(
        ccre_state_path=args.ccre_state,
        ccre_state_format=args.ccre_state_format,
        assay_source_path=args.assay_source,
        assay_source_format=args.assay_source_format,
        output_path=output_path,
        output_format=args.output_format,
    )

    run_manifest_path = _write_run_manifest(
        command_name="build-validation-link",
        output_dir=output_dir,
        inputs={"ccre_state": str(Path(args.ccre_state).resolve()), "assay_source": str(Path(args.assay_source).resolve())},
        params={
            "ccre_state_format": args.ccre_state_format,
            "assay_source_format": args.assay_source_format,
            "output_format": args.output_format,
        },
        outputs={"validation_link": str(result.output_path.resolve())},
        row_count=result.row_count,
    )

    print(f"build-validation-link_complete rows={result.row_count} output={result.output_path}")
    print(f"run_manifest={run_manifest_path}")
    return 0


def _handle_build_holdouts(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    extension = _artifact_extension(args.output_format)

    publication_path = output_dir / f"validation_link_publication.{extension}"
    locus_path = output_dir / f"validation_link_locus.{extension}"

    result = run_holdout_build(
        validation_link_path=args.validation_link,
        validation_link_format=args.validation_link_format,
        publication_output_path=publication_path,
        locus_output_path=locus_path,
        output_format=args.output_format,
    )

    holdout_summary_path = output_dir / "holdout_summary.json"
    _write_json(
        holdout_summary_path,
        {
            "publication_row_count": result.publication_row_count,
            "locus_row_count": result.locus_row_count,
            "leakage_audited": True,
        },
    )

    run_manifest_path = _write_run_manifest(
        command_name="build-holdouts",
        output_dir=output_dir,
        inputs={"validation_link": str(Path(args.validation_link).resolve())},
        params={"validation_link_format": args.validation_link_format, "output_format": args.output_format},
        outputs={
            "publication_validation_link": str(result.publication_path.resolve()),
            "locus_validation_link": str(result.locus_path.resolve()),
            "holdout_summary": str(holdout_summary_path.resolve()),
        },
        row_count=result.publication_row_count,
    )

    print(f"build-holdouts_complete publication={result.publication_path} locus={result.locus_path}")
    print(f"holdout_summary={holdout_summary_path}")
    print(f"run_manifest={run_manifest_path}")
    return 0


def _handle_evaluate_ranking(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    pub_report = output_dir / "ranking_publication_report.json"
    pub_scores = output_dir / "ranking_publication_scores.jsonl"
    loc_report = output_dir / "ranking_locus_report.json"
    loc_scores = output_dir / "ranking_locus_scores.jsonl"

    run_ranking_evaluation(
        feature_matrix_path=args.feature_matrix,
        feature_matrix_format=args.feature_matrix_format,
        validation_link_path=args.publication_validation,
        validation_link_format=args.publication_validation_format,
        report_output_path=pub_report,
        scores_output_path=pub_scores,
    )

    run_ranking_evaluation(
        feature_matrix_path=args.feature_matrix,
        feature_matrix_format=args.feature_matrix_format,
        validation_link_path=args.locus_validation,
        validation_link_format=args.locus_validation_format,
        report_output_path=loc_report,
        scores_output_path=loc_scores,
    )

    pub_metrics = json.loads(pub_report.read_text(encoding="utf-8"))
    loc_metrics = json.loads(loc_report.read_text(encoding="utf-8"))

    comparison_path = output_dir / "baseline_comparison.json"
    _write_json(
        comparison_path,
        {
            "publication": {
                "top_k": pub_metrics.get("top_k", {}),
                "pr_auc": pub_metrics.get("pr_auc", {}),
            },
            "locus": {
                "top_k": loc_metrics.get("top_k", {}),
                "pr_auc": loc_metrics.get("pr_auc", {}),
            },
        },
    )

    run_manifest_path = _write_run_manifest(
        command_name="evaluate-ranking",
        output_dir=output_dir,
        inputs={
            "feature_matrix": str(Path(args.feature_matrix).resolve()),
            "publication_validation": str(Path(args.publication_validation).resolve()),
            "locus_validation": str(Path(args.locus_validation).resolve()),
        },
        params={
            "feature_matrix_format": args.feature_matrix_format,
            "publication_validation_format": args.publication_validation_format,
            "locus_validation_format": args.locus_validation_format,
        },
        outputs={
            "ranking_publication_report": str(pub_report.resolve()),
            "ranking_publication_scores": str(pub_scores.resolve()),
            "ranking_locus_report": str(loc_report.resolve()),
            "ranking_locus_scores": str(loc_scores.resolve()),
            "baseline_comparison": str(comparison_path.resolve()),
        },
    )

    print(f"evaluate-ranking_complete comparison={comparison_path}")
    print(f"run_manifest={run_manifest_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "fetch-source":
        return _handle_fetch_source(args)
    if args.command == "ingest-ccre":
        return _handle_ingest_ccre(args)
    if args.command == "smoke-ingest":
        return _handle_smoke_ingest(args)
    if args.command == "project-fixture":
        return _handle_project_fixture(args)
    if args.command == "call-states":
        return _handle_call_states(args)
    if args.command == "discover-candidates":
        return _handle_discover_candidates(args)
    if args.command == "featurize":
        return _handle_featurize(args)
    if args.command == "build-validation-link":
        return _handle_build_validation_link(args)
    if args.command == "build-holdouts":
        return _handle_build_holdouts(args)
    if args.command == "evaluate-ranking":
        return _handle_evaluate_ranking(args)

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
