#!/usr/bin/env python3
"""CLI for source manifest creation and validation."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from panccre.manifests import build_manifest_entry, load_manifest_file, write_manifest_file


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create and validate pan-ccre source manifests")
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create", help="Create a manifest JSON from a source file")
    create.add_argument("--file", required=True, help="Path to downloaded source file")
    create.add_argument("--output", required=True, help="Output manifest JSON path")
    create.add_argument("--source-id", required=True)
    create.add_argument("--name")
    create.add_argument("--version", required=True)
    create.add_argument("--download-url", required=True)
    create.add_argument("--download-date", help="Override download date (YYYY-MM-DD)")
    create.add_argument("--license", required=True, dest="license_name")
    create.add_argument("--genome-build", required=True)
    create.add_argument("--parser-version", required=True)
    create.add_argument("--format", dest="file_format")
    create.add_argument("--notes")

    validate = subparsers.add_parser("validate", help="Validate an existing manifest JSON")
    validate.add_argument("--manifest", required=True, help="Manifest JSON path")

    return parser


def _handle_create(args: argparse.Namespace) -> int:
    manifest = build_manifest_entry(
        file_path=args.file,
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
    output_path = write_manifest_file(manifest, args.output)
    print(f"manifest_written={output_path}")
    return 0


def _handle_validate(args: argparse.Namespace) -> int:
    manifest = load_manifest_file(args.manifest)
    print(f"manifest_valid source_id={manifest.source_id} version={manifest.version}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "create":
        return _handle_create(args)
    if args.command == "validate":
        return _handle_validate(args)

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
