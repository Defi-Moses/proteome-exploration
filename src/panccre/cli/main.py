"""CLI entrypoints for pan-ccre developer workflows."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from panccre.ingest import ingest_ccre_ref
from panccre.manifests import load_manifest_file


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _smoke_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("smoke-ingest", help="Run fixture manifest validation + cCRE ingest")
    parser.add_argument(
        "--fixture-bed",
        default=str(_repo_root() / "tests" / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"),
        help="Fixture BED path",
    )
    parser.add_argument("--manifest", help="Optional manifest JSON path to validate before ingest")
    parser.add_argument(
        "--output-dir",
        default=str(_repo_root() / "data" / "interim" / "smoke"),
        help="Output directory",
    )
    parser.add_argument("--context-group", default="immune_hematopoietic")
    parser.add_argument("--source-release", default="fixture-2026-03")
    parser.add_argument("--output-format", choices=["parquet", "csv", "jsonl"], default="jsonl")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="pan-ccre CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _smoke_parser(subparsers)
    return parser


def _write_run_manifest(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run_smoke_ingest(args: argparse.Namespace) -> int:
    fixture_path = Path(args.fixture_bed)
    output_dir = Path(args.output_dir)

    if not fixture_path.exists():
        raise FileNotFoundError(f"Fixture BED not found: {fixture_path}")

    validated_manifest_path: str | None = None
    if args.manifest:
        manifest = load_manifest_file(args.manifest)
        validated_manifest_path = str(Path(args.manifest).resolve())
        print(f"manifest_validated source_id={manifest.source_id} version={manifest.version}")

    extension = "parquet" if args.output_format == "parquet" else args.output_format
    output_path = output_dir / f"ccre_ref.{extension}"

    result = ingest_ccre_ref(
        bed_path=fixture_path,
        output_path=output_path,
        context_group=args.context_group,
        source_release=args.source_release,
        output_format=args.output_format,
    )

    run_manifest = {
        "command": "smoke-ingest",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            "fixture_bed": str(fixture_path.resolve()),
            "manifest": validated_manifest_path,
        },
        "params": {
            "context_group": args.context_group,
            "source_release": args.source_release,
            "output_format": args.output_format,
        },
        "outputs": {
            "ccre_ref": str(result.output_path.resolve()),
        },
        "row_count": result.row_count,
    }
    run_manifest_path = output_dir / "run_manifest.json"
    _write_run_manifest(run_manifest_path, run_manifest)

    print(f"smoke_ingest_complete rows={result.row_count} output={result.output_path}")
    print(f"run_manifest={run_manifest_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "smoke-ingest":
        return _run_smoke_ingest(args)

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
