"""Build and validate manifest files for raw sources."""

from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
from typing import Any

from panccre.manifests.schema import SourceManifest, manifest_from_dict


def compute_sha256(file_path: str | Path, chunk_size: int = 1024 * 1024) -> str:
    """Compute the SHA256 checksum for a local file."""
    path = Path(file_path)
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)

    return digest.hexdigest()


def build_manifest_entry(
    *,
    file_path: str | Path,
    source_id: str,
    version: str,
    download_url: str,
    license_name: str,
    genome_build: str,
    parser_version: str,
    download_date: str | None = None,
    name: str | None = None,
    file_format: str | None = None,
    notes: str | None = None,
) -> SourceManifest:
    """Create a validated source manifest from a local file and metadata."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file does not exist: {path}")

    payload: dict[str, Any] = {
        "source_id": source_id,
        "version": version,
        "download_url": download_url,
        "download_date": download_date or date.today().isoformat(),
        "checksum": compute_sha256(path),
        "license": license_name,
        "genome_build": genome_build,
        "parser_version": parser_version,
        "name": name,
        "format": file_format,
        "notes": notes,
    }
    return manifest_from_dict(payload)


def write_manifest_file(manifest: SourceManifest, output_path: str | Path) -> Path:
    """Write a single-manifest JSON file."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def load_manifest_file(path: str | Path) -> SourceManifest:
    """Load and validate a JSON manifest file."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Manifest payload must be a JSON object")
    return manifest_from_dict(payload)
