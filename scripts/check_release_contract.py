#!/usr/bin/env python3
"""Validate release manifest artifact integrity."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from panccre.manifests.builder import compute_sha256


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Check release_manifest.json file and artifact checksums")
    parser.add_argument("--release-manifest", required=True)
    return parser


def _load_manifest(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("release manifest must be a JSON object")
    return payload


def check_release_manifest(path: str | Path) -> None:
    manifest_path = Path(path)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Release manifest not found: {manifest_path}")

    payload = _load_manifest(manifest_path)
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("release manifest must include a non-empty artifacts list")

    for record in artifacts:
        if not isinstance(record, dict):
            raise ValueError("artifact entry must be an object")
        raw_path = record.get("path")
        expected_sha = record.get("sha256")
        expected_bytes = record.get("bytes")
        if not isinstance(raw_path, str) or not raw_path:
            raise ValueError("artifact.path must be a non-empty string")
        if not isinstance(expected_sha, str) or not expected_sha:
            raise ValueError("artifact.sha256 must be a non-empty string")
        if not isinstance(expected_bytes, int):
            raise ValueError("artifact.bytes must be an integer")

        artifact_path = Path(raw_path)
        if not artifact_path.exists():
            raise FileNotFoundError(f"Artifact missing: {artifact_path}")
        actual_bytes = int(artifact_path.stat().st_size)
        actual_sha = compute_sha256(artifact_path)

        if actual_bytes != expected_bytes:
            raise ValueError(f"Artifact byte-size mismatch for {artifact_path}: expected={expected_bytes} actual={actual_bytes}")
        if actual_sha != expected_sha:
            raise ValueError(f"Artifact checksum mismatch for {artifact_path}: expected={expected_sha} actual={actual_sha}")

    print(f"release_contract_ok artifacts={len(artifacts)} manifest={manifest_path}")


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    check_release_manifest(args.release_manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
