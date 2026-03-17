#!/usr/bin/env python3
"""CLI for source manifest creation, validation, and fetch+lock workflows."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from panccre.manifests import (
    build_manifest_entry,
    fetch_source_artifact,
    load_manifest_file,
    upsert_manifest_lock_entry,
    write_manifest_file,
)


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

    fetch = subparsers.add_parser(
        "fetch",
        help="Download source, write manifest file, and update immutable lockfile",
    )
    fetch.add_argument("--download-url", required=True)
    fetch.add_argument("--source-id", required=True)
    fetch.add_argument("--name")
    fetch.add_argument("--version", required=True)
    fetch.add_argument("--download-date", help="Override download date (YYYY-MM-DD)")
    fetch.add_argument("--license", required=True, dest="license_name")
    fetch.add_argument("--genome-build", required=True)
    fetch.add_argument("--parser-version", required=True)
    fetch.add_argument("--format", dest="file_format")
    fetch.add_argument("--notes")
    fetch.add_argument("--filename", help="Override artifact filename in raw storage")
    fetch.add_argument(
        "--raw-root",
        default=str(ROOT / "data" / "raw"),
        help="Root directory for raw artifacts",
    )
    fetch.add_argument(
        "--manifest-root",
        default=str(ROOT / "data" / "raw" / "manifests"),
        help="Root directory for per-source manifest JSON files",
    )
    fetch.add_argument(
        "--lock-file",
        default=str(ROOT / "data" / "raw" / "manifests" / "manifest.lock.json"),
        help="Manifest lockfile path",
    )

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


def _handle_fetch(args: argparse.Namespace) -> int:
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


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "create":
        return _handle_create(args)
    if args.command == "validate":
        return _handle_validate(args)
    if args.command == "fetch":
        return _handle_fetch(args)

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
