"""Manifest lockfile helpers for immutable source tracking."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

from panccre.manifests.builder import compute_sha256
from panccre.manifests.schema import SourceManifest

LOCKFILE_VERSION = 1


def _default_lock_payload() -> dict[str, Any]:
    return {
        "version": LOCKFILE_VERSION,
        "entries": {},
    }


def load_manifest_lock(lock_path: str | Path) -> dict[str, Any]:
    """Load lockfile payload or create default in-memory payload."""
    path = Path(lock_path)
    if not path.exists():
        return _default_lock_payload()

    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Manifest lockfile payload must be a JSON object")
    if payload.get("version") != LOCKFILE_VERSION:
        raise ValueError(f"Unsupported lockfile version: {payload.get('version')}")
    if not isinstance(payload.get("entries"), dict):
        raise ValueError("Manifest lockfile must contain an 'entries' object")

    return payload


def write_manifest_lock(payload: dict[str, Any], lock_path: str | Path) -> Path:
    """Write lockfile payload to disk."""
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def manifest_lock_key(manifest: SourceManifest) -> str:
    """Build stable lock key for a manifest row."""
    return f"{manifest.source_id}@{manifest.version}"


def ensure_artifact_matches_checksum(artifact_path: str | Path, expected_checksum: str) -> str:
    """Validate file checksum and return computed checksum."""
    path = Path(artifact_path)
    if not path.exists():
        raise FileNotFoundError(f"Artifact does not exist: {path}")

    actual = compute_sha256(path)
    if actual != expected_checksum:
        raise ValueError(
            f"Checksum drift detected for {path}: expected={expected_checksum} actual={actual}"
        )
    return actual


def upsert_manifest_lock_entry(
    *,
    lock_path: str | Path,
    manifest: SourceManifest,
    artifact_path: str | Path,
) -> Path:
    """Insert or verify a manifest lock entry.

    If an entry already exists with a different checksum, this raises to enforce
    immutable source tracking.
    """
    payload = load_manifest_lock(lock_path)
    key = manifest_lock_key(manifest)
    entries = payload["entries"]

    ensure_artifact_matches_checksum(artifact_path, manifest.checksum)

    existing = entries.get(key)
    if existing is not None and existing.get("checksum") != manifest.checksum:
        raise ValueError(
            f"Lockfile conflict for {key}: existing checksum {existing.get('checksum')} "
            f"!= new checksum {manifest.checksum}"
        )

    artifact = Path(artifact_path)
    entries[key] = {
        "source_id": manifest.source_id,
        "version": manifest.version,
        "checksum": manifest.checksum,
        "artifact_path": str(artifact.resolve()),
        "file_size_bytes": artifact.stat().st_size,
        "manifest": manifest.to_dict(),
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    return write_manifest_lock(payload, lock_path)
