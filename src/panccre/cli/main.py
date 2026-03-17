"""CLI entrypoints for pan-ccre developer workflows."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

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
    parser.add_argument(
        "--raw-root",
        default=str(_repo_root() / "data" / "raw"),
    )
    parser.add_argument(
        "--manifest-root",
        default=str(_repo_root() / "data" / "raw" / "manifests"),
    )
    parser.add_argument(
        "--lock-file",
        default=str(_repo_root() / "data" / "raw" / "manifests" / "manifest.lock.json"),
    )


def _add_ingest_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "ingest-ccre",
        help="Ingest BED-like cCRE source into canonical ccre_ref output",
    )
    parser.add_argument("--input-bed", required=True)
    parser.add_argument("--manifest", help="Optional manifest to validate against input checksum")
    parser.add_argument("--context-group", default="immune_hematopoietic")
    parser.add_argument("--source-release", required=True)
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="parquet")
    parser.add_argument(
        "--output-dir",
        default=str(_repo_root() / "data" / "processed" / "ccre_ref"),
    )
    parser.add_argument("--output", help="Optional explicit output file path")


def _add_smoke_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("smoke-ingest", help="Run fixture manifest validation + cCRE ingest")
    parser.add_argument(
        "--fixture-bed",
        default=str(_repo_root() / "tests" / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"),
    )
    parser.add_argument("--manifest", help="Optional manifest JSON path to validate before ingest")
    parser.add_argument(
        "--output-dir",
        default=str(_repo_root() / "data" / "interim" / "smoke"),
    )
    parser.add_argument("--context-group", default="immune_hematopoietic")
    parser.add_argument("--source-release", default="fixture-2026-03")
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="jsonl")


def _add_project_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "project-fixture",
        help="Build fixture hap_projection from ccre_ref and haplotype IDs",
    )
    parser.add_argument(
        "--ccre-ref",
        default=str(_repo_root() / "data" / "interim" / "smoke" / "ccre_ref.jsonl"),
    )
    parser.add_argument("--ccre-ref-format", choices=["parquet", "csv", "jsonl"], default=None)
    parser.add_argument(
        "--haplotypes",
        default=str(_repo_root() / "tests" / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"),
    )
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="jsonl")
    parser.add_argument(
        "--output-dir",
        default=str(_repo_root() / "data" / "interim" / "projection"),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="pan-ccre CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_fetch_source_parser(subparsers)
    _add_ingest_parser(subparsers)
    _add_smoke_parser(subparsers)
    _add_project_parser(subparsers)
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


def _run_ingest(
    *,
    input_bed: str,
    manifest_path: str | None,
    output_dir: str,
    context_group: str,
    source_release: str,
    output_format: str,
    explicit_output: str | None,
    command_name: str,
) -> int:
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

    run_manifest = {
        "command": command_name,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "input_bed": str(bed_path.resolve()),
            "manifest": validated_manifest_path,
        },
        "params": {
            "context_group": context_group,
            "source_release": source_release,
            "output_format": output_format,
        },
        "outputs": {
            "ccre_ref": str(result.output_path.resolve()),
        },
        "row_count": result.row_count,
    }
    run_manifest_path = Path(output_dir) / "run_manifest.json"
    _write_json(run_manifest_path, run_manifest)

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

    run_manifest = {
        "command": "project-fixture",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "ccre_ref": str(Path(args.ccre_ref).resolve()),
            "haplotypes": str(Path(args.haplotypes).resolve()),
        },
        "params": {
            "ccre_ref_format": args.ccre_ref_format,
            "output_format": args.output_format,
        },
        "outputs": {
            "hap_projection": str(result.output_path.resolve()),
            "qc_summary": str(result.qc_summary_path.resolve()),
        },
        "row_count": result.row_count,
    }
    run_manifest_path = output_dir / "run_manifest.json"
    _write_json(run_manifest_path, run_manifest)

    print(f"project-fixture_complete rows={result.row_count} output={result.output_path}")
    print(f"qc_summary={result.qc_summary_path}")
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

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
