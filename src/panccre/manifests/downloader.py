"""Raw source download helpers for manifest workflows."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import tempfile
from urllib.parse import unquote, urlparse
from urllib.request import urlopen


@dataclass(frozen=True)
class DownloadResult:
    """Details about a source download operation."""

    artifact_path: Path
    bytes_written: int
    reused_existing: bool


def _filename_from_url(download_url: str) -> str | None:
    parsed = urlparse(download_url)
    if parsed.path:
        name = Path(unquote(parsed.path)).name
        if name:
            return name
    return None


def _source_path_for_file_url(download_url: str) -> Path:
    parsed = urlparse(download_url)
    if parsed.scheme == "file":
        return Path(unquote(parsed.path))
    if parsed.scheme == "":
        return Path(download_url)
    raise ValueError(f"Unsupported local source URL scheme: {parsed.scheme}")


def _download_http_to_path(download_url: str, destination: Path) -> int:
    with urlopen(download_url) as response, destination.open("wb") as out:
        written = 0
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            out.write(chunk)
            written += len(chunk)
    return written


def fetch_source_artifact(
    *,
    download_url: str,
    raw_root: str | Path,
    source_id: str,
    version: str,
    filename: str | None = None,
) -> DownloadResult:
    """Fetch a source artifact into `data/raw/<source_id>/<version>/`.

    Supports `file://`, plain local paths, and `http(s)` URLs.
    """
    file_name = filename or _filename_from_url(download_url) or f"{source_id}_{version}.dat"
    target_dir = Path(raw_root) / source_id / version
    target_dir.mkdir(parents=True, exist_ok=True)

    artifact_path = target_dir / file_name
    if artifact_path.exists():
        return DownloadResult(
            artifact_path=artifact_path,
            bytes_written=artifact_path.stat().st_size,
            reused_existing=True,
        )

    file_descriptor, temp_name = tempfile.mkstemp(prefix="download_", suffix=".tmp", dir=target_dir)
    os.close(file_descriptor)
    temp_file = Path(temp_name)
    try:
        parsed = urlparse(download_url)
        if parsed.scheme in {"http", "https"}:
            bytes_written = _download_http_to_path(download_url, temp_file)
        else:
            source_path = _source_path_for_file_url(download_url)
            if not source_path.exists():
                raise FileNotFoundError(f"Source artifact not found: {source_path}")
            shutil.copy2(source_path, temp_file)
            bytes_written = temp_file.stat().st_size

        temp_file.replace(artifact_path)
        return DownloadResult(
            artifact_path=artifact_path,
            bytes_written=bytes_written,
            reused_existing=False,
        )
    except Exception:
        if temp_file.exists():
            temp_file.unlink()
        raise
