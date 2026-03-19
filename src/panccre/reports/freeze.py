"""Freeze holdout splits and ranking reports under a version label."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil

_REQUIRED_VALIDATION_FILES = (
    "validation_link_publication.jsonl",
    "validation_link_locus.jsonl",
    "holdout_summary.json",
)

_REQUIRED_RANKING_FILES = (
    "ranking_publication_report.json",
    "ranking_locus_report.json",
    "baseline_comparison.json",
)


@dataclass(frozen=True)
class FreezeEvaluationResult:
    label: str
    validation_dir: Path
    ranking_dir: Path
    manifest_path: Path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _copy_required_files(source_dir: Path, output_dir: Path, required_files: tuple[str, ...]) -> list[Path]:
    copied: list[Path] = []
    output_dir.mkdir(parents=True, exist_ok=True)

    for filename in required_files:
        source_path = source_dir / filename
        if not source_path.exists():
            raise FileNotFoundError(f"Required freeze input missing: {source_path}")
        target_path = output_dir / filename
        shutil.copy2(source_path, target_path)
        copied.append(target_path)

    return copied


def freeze_evaluation(
    *,
    label: str,
    validation_source_dir: str | Path,
    ranking_source_dir: str | Path,
    output_root: str | Path,
) -> FreezeEvaluationResult:
    normalized = str(label).strip()
    if not normalized:
        raise ValueError("freeze label must not be empty")
    if "/" in normalized or "\\" in normalized:
        raise ValueError("freeze label must not contain path separators")

    validation_source = Path(validation_source_dir)
    ranking_source = Path(ranking_source_dir)
    root = Path(output_root)

    target_validation = root / "validation" / "frozen" / normalized
    target_ranking = root / "ranking" / "frozen" / normalized
    target_manifest = root / "frozen" / normalized / "freeze_manifest.json"

    if target_validation.exists() or target_ranking.exists() or target_manifest.exists():
        raise FileExistsError(f"freeze label already exists: {normalized}")

    copied_validation = _copy_required_files(validation_source, target_validation, _REQUIRED_VALIDATION_FILES)
    copied_ranking = _copy_required_files(ranking_source, target_ranking, _REQUIRED_RANKING_FILES)

    records: list[dict[str, object]] = []
    for path in copied_validation + copied_ranking:
        stat = path.stat()
        records.append(
            {
                "path": str(path.resolve()),
                "bytes": int(stat.st_size),
                "sha256": _sha256(path),
            }
        )

    target_manifest.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "label": normalized,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "validation_source_dir": str(validation_source.resolve()),
            "ranking_source_dir": str(ranking_source.resolve()),
        },
        "artifacts": records,
    }
    target_manifest.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return FreezeEvaluationResult(
        label=normalized,
        validation_dir=target_validation,
        ranking_dir=target_ranking,
        manifest_path=target_manifest,
    )
