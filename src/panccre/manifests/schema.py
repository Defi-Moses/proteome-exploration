"""Schema and validation helpers for source manifests."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
import re
from typing import Any

REQUIRED_FIELDS = (
    "source_id",
    "version",
    "download_url",
    "download_date",
    "checksum",
    "license",
    "genome_build",
    "parser_version",
)

_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


@dataclass(frozen=True)
class SourceManifest:
    """Normalized source manifest contract."""

    source_id: str
    version: str
    download_url: str
    download_date: str
    checksum: str
    license: str
    genome_build: str
    parser_version: str
    name: str | None = None
    format: str | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable manifest dictionary without null values."""
        data = asdict(self)
        return {key: value for key, value in data.items() if value is not None}


class ManifestValidationError(ValueError):
    """Raised when a source manifest fails validation."""


def validate_manifest_dict(payload: dict[str, Any]) -> list[str]:
    """Validate a raw manifest dictionary and return a list of errors."""
    errors: list[str] = []

    for field in REQUIRED_FIELDS:
        if field not in payload:
            errors.append(f"Missing required field: {field}")
            continue
        value = payload[field]
        if not isinstance(value, str) or not value.strip():
            errors.append(f"Field '{field}' must be a non-empty string")

    checksum = payload.get("checksum", "")
    if isinstance(checksum, str) and checksum and not _SHA256_RE.match(checksum):
        errors.append("Field 'checksum' must be a lowercase 64-character sha256 hex string")

    download_date = payload.get("download_date", "")
    if isinstance(download_date, str) and download_date:
        try:
            date.fromisoformat(download_date)
        except ValueError:
            errors.append("Field 'download_date' must be ISO-8601 YYYY-MM-DD")

    return errors


def manifest_from_dict(payload: dict[str, Any]) -> SourceManifest:
    """Construct a `SourceManifest` from validated dictionary data."""
    errors = validate_manifest_dict(payload)
    if errors:
        raise ManifestValidationError("; ".join(errors))

    return SourceManifest(
        source_id=payload["source_id"].strip(),
        version=payload["version"].strip(),
        download_url=payload["download_url"].strip(),
        download_date=payload["download_date"].strip(),
        checksum=payload["checksum"].strip(),
        license=payload["license"].strip(),
        genome_build=payload["genome_build"].strip(),
        parser_version=payload["parser_version"].strip(),
        name=payload.get("name"),
        format=payload.get("format"),
        notes=payload.get("notes"),
    )
