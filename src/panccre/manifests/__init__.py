"""Manifest tooling for source metadata and provenance."""

from panccre.manifests.builder import (
    build_manifest_entry,
    compute_sha256,
    load_manifest_file,
    write_manifest_file,
)
from panccre.manifests.downloader import DownloadResult, fetch_source_artifact
from panccre.manifests.lockfile import (
    ensure_artifact_matches_checksum,
    load_manifest_lock,
    manifest_lock_key,
    upsert_manifest_lock_entry,
    write_manifest_lock,
)
from panccre.manifests.schema import ManifestValidationError, SourceManifest, validate_manifest_dict

__all__ = [
    "DownloadResult",
    "ManifestValidationError",
    "SourceManifest",
    "build_manifest_entry",
    "compute_sha256",
    "ensure_artifact_matches_checksum",
    "fetch_source_artifact",
    "load_manifest_file",
    "load_manifest_lock",
    "manifest_lock_key",
    "upsert_manifest_lock_entry",
    "validate_manifest_dict",
    "write_manifest_file",
    "write_manifest_lock",
]
