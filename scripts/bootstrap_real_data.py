#!/usr/bin/env python3
"""Bootstrap raw source downloads + manifests from a YAML source plan."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from panccre.manifests import (
    build_manifest_entry,
    fetch_source_artifact,
    upsert_manifest_lock_entry,
    write_manifest_file,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bootstrap phase-1 source downloads and manifest lock entries")
    parser.add_argument("--config", default=str(ROOT / "configs" / "sources" / "phase1_sources.yaml"))
    parser.add_argument("--raw-root", default=str(ROOT / "data" / "raw"))
    parser.add_argument("--manifest-root", default=str(ROOT / "data" / "raw" / "manifests"))
    parser.add_argument("--lock-file", default=str(ROOT / "data" / "raw" / "manifests" / "manifest.lock.json"))
    parser.add_argument("--execute", action="store_true", help="Run downloads and writes. Without this flag, only print a dry-run.")
    return parser


def _load_config(path: Path) -> list[dict[str, Any]]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Source config must be a YAML object")
    sources = payload.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ValueError("Source config must include a non-empty 'sources' list")

    normalized: list[dict[str, Any]] = []
    required_fields = [
        "source_id",
        "version",
        "download_url",
        "license",
        "genome_build",
        "parser_version",
    ]
    for idx, source in enumerate(sources):
        if not isinstance(source, dict):
            raise ValueError(f"sources[{idx}] must be an object")
        if "enabled" in source and not isinstance(source.get("enabled"), bool):
            raise ValueError(f"sources[{idx}] field 'enabled' must be a boolean when provided")
        for field in required_fields:
            value = source.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"sources[{idx}] missing required string field '{field}'")
        normalized.append(source)
    return normalized


def _has_placeholder(value: str) -> bool:
    return "<" in value or ">" in value


def bootstrap_sources(
    *,
    config_path: str | Path,
    raw_root: str | Path,
    manifest_root: str | Path,
    lock_file: str | Path,
    execute: bool,
) -> Path:
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Source config not found: {config_file}")

    sources = _load_config(config_file)
    summary_rows: list[dict[str, Any]] = []

    for source in sources:
        source_id = str(source["source_id"]).strip()
        version = str(source["version"]).strip()
        enabled = bool(source.get("enabled", True))
        download_url = str(source["download_url"]).strip()

        action = {
            "source_id": source_id,
            "version": version,
            "download_url": download_url,
            "execute": bool(execute),
            "enabled": enabled,
        }

        if not enabled:
            print(f"skipped source={source_id}@{version} reason=disabled")
            action["skipped"] = True
            action["skip_reason"] = "disabled"
            summary_rows.append(action)
            continue

        if _has_placeholder(download_url):
            raise ValueError(f"download_url for {source_id}@{version} still contains placeholders: {download_url}")

        if not execute:
            print(f"dry_run source={source_id}@{version} url={download_url}")
            summary_rows.append(action)
            continue

        download = fetch_source_artifact(
            download_url=download_url,
            raw_root=raw_root,
            source_id=source_id,
            version=version,
            filename=source.get("filename"),
        )

        manifest = build_manifest_entry(
            file_path=download.artifact_path,
            source_id=source_id,
            version=version,
            download_url=download_url,
            license_name=str(source["license"]),
            genome_build=str(source["genome_build"]),
            parser_version=str(source["parser_version"]),
            download_date=source.get("download_date"),
            name=source.get("name"),
            file_format=source.get("format"),
            notes=source.get("notes"),
        )

        manifest_path = Path(manifest_root) / source_id / f"{version}.json"
        write_manifest_file(manifest, manifest_path)

        upsert_manifest_lock_entry(
            lock_path=lock_file,
            manifest=manifest,
            artifact_path=download.artifact_path,
        )

        action["artifact_path"] = str(download.artifact_path.resolve())
        action["artifact_reused"] = bool(download.reused_existing)
        action["checksum"] = manifest.checksum
        action["manifest_path"] = str(manifest_path.resolve())

        print(f"fetched source={source_id}@{version} artifact={download.artifact_path}")
        summary_rows.append(action)

    summary = {
        "config_path": str(config_file.resolve()),
        "execute": bool(execute),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "sources": summary_rows,
    }
    summary_path = Path(manifest_root) / "bootstrap_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"bootstrap_summary={summary_path}")
    return summary_path


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    bootstrap_sources(
        config_path=args.config,
        raw_root=args.raw_root,
        manifest_root=args.manifest_root,
        lock_file=args.lock_file,
        execute=args.execute,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
