"""Fixture-based haplotype projection for early pipeline validation."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

import pandas as pd

from panccre.ingest import read_ccre_ref

HAP_PROJECTION_COLUMNS = [
    "ccre_id",
    "haplotype_id",
    "ref_chr",
    "ref_start",
    "ref_end",
    "alt_contig",
    "alt_start",
    "alt_end",
    "orientation",
    "map_status",
    "coverage_frac",
    "seq_identity",
    "split_count",
    "copy_count",
    "flank_synteny_confidence",
    "mapping_method",
]

_STATUS_CYCLE = [
    "exact",
    "exact",
    "diverged",
    "fractured",
    "absent",
    "duplicated",
    "exact",
    "diverged",
    "ambiguous",
    "exact",
]


@dataclass(frozen=True)
class ProjectionResult:
    """Summary of a projection run."""

    row_count: int
    output_path: Path
    qc_summary_path: Path
    output_format: str


def _parquet_available() -> bool:
    try:
        pd.io.parquet.get_engine("auto")
        return True
    except Exception:
        return False


def load_haplotype_ids(path: str | Path) -> list[str]:
    """Load haplotype IDs from a one-column TSV fixture."""
    file_path = Path(path)
    ids: list[str] = []
    seen: set[str] = set()

    with file_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip()
            if not value:
                continue
            if value.startswith("#"):
                continue
            if value.lower() == "haplotype_id":
                continue
            if value in seen:
                raise ValueError(f"Duplicate haplotype_id in {file_path}: {value}")
            seen.add(value)
            ids.append(value)

    if not ids:
        raise ValueError(f"No haplotype IDs found in {file_path}")

    return ids


def _status_values(map_status: str) -> tuple[float, float, int, int, float, str]:
    if map_status == "exact":
        return (0.99, 0.998, 1, 1, 0.98, "+")
    if map_status == "diverged":
        return (0.84, 0.955, 1, 1, 0.92, "+")
    if map_status == "fractured":
        return (0.61, 0.931, 2, 1, 0.77, "+")
    if map_status == "absent":
        return (0.0, 0.0, 0, 0, 0.35, ".")
    if map_status == "duplicated":
        return (0.98, 0.997, 1, 2, 0.96, "+")
    return (0.72, 0.941, 1, 1, 0.58, ".")


def build_fixture_hap_projection(
    *,
    ccre_ref_path: str | Path,
    haplotypes_path: str | Path,
    ccre_ref_format: str | None = None,
) -> pd.DataFrame:
    """Build deterministic hap_projection rows from fixture inputs."""
    ccre_ref = read_ccre_ref(ccre_ref_path, input_format=ccre_ref_format)
    haplotypes = load_haplotype_ids(haplotypes_path)

    rows: list[dict[str, object]] = []
    for ccre_index, ccre_row in ccre_ref.reset_index(drop=True).iterrows():
        for hap_index, haplotype_id in enumerate(haplotypes):
            map_status = _STATUS_CYCLE[(ccre_index + hap_index) % len(_STATUS_CYCLE)]
            coverage_frac, seq_identity, split_count, copy_count, flank_conf, orientation = _status_values(map_status)

            alt_contig: str | None = ccre_row["chr"]
            alt_start: int | None = int(ccre_row["start"])
            alt_end: int | None = int(ccre_row["end"])

            if map_status == "absent":
                alt_contig = None
                alt_start = None
                alt_end = None
            elif map_status == "diverged":
                alt_start = int(ccre_row["start"]) + 5
                alt_end = int(ccre_row["end"]) + 5
            elif map_status == "fractured":
                alt_end = int(ccre_row["start"]) + int(ccre_row["anchor_width"]) // 2
            elif map_status == "duplicated":
                alt_start = int(ccre_row["start"]) - 3
                alt_end = int(ccre_row["end"]) - 3

            rows.append(
                {
                    "ccre_id": ccre_row["ccre_id"],
                    "haplotype_id": haplotype_id,
                    "ref_chr": ccre_row["chr"],
                    "ref_start": int(ccre_row["start"]),
                    "ref_end": int(ccre_row["end"]),
                    "alt_contig": alt_contig,
                    "alt_start": alt_start,
                    "alt_end": alt_end,
                    "orientation": orientation,
                    "map_status": map_status,
                    "coverage_frac": coverage_frac,
                    "seq_identity": seq_identity,
                    "split_count": split_count,
                    "copy_count": copy_count,
                    "flank_synteny_confidence": flank_conf,
                    "mapping_method": "fixture_projection_v1",
                }
            )

    frame = pd.DataFrame(rows, columns=HAP_PROJECTION_COLUMNS)
    validate_hap_projection_frame(frame)
    return frame


def validate_hap_projection_frame(frame: pd.DataFrame) -> None:
    """Strict contract guard for hap_projection rows."""
    actual_columns = list(frame.columns)
    if actual_columns != HAP_PROJECTION_COLUMNS:
        raise ValueError(
            "hap_projection column contract mismatch: "
            f"expected={HAP_PROJECTION_COLUMNS} actual={actual_columns}"
        )

    if frame.empty:
        raise ValueError("hap_projection frame must not be empty")

    required_non_null = [
        "ccre_id",
        "haplotype_id",
        "ref_chr",
        "ref_start",
        "ref_end",
        "orientation",
        "map_status",
        "coverage_frac",
        "seq_identity",
        "split_count",
        "copy_count",
        "flank_synteny_confidence",
        "mapping_method",
    ]
    for column in required_non_null:
        if frame[column].isna().any():
            raise ValueError(f"hap_projection contains null values in {column}")

    if (frame["ref_end"] <= frame["ref_start"]).any():
        raise ValueError("hap_projection contains ref_end <= ref_start rows")


def _write_projection_frame(frame: pd.DataFrame, path: Path, output_format: str) -> None:
    output_format = output_format.lower()
    if output_format == "parquet":
        if not _parquet_available():
            raise RuntimeError(
                "Parquet output requires pyarrow or fastparquet. "
                "Install one of those engines or choose --output-format csv/jsonl."
            )
        frame.to_parquet(path, index=False)
    elif output_format == "csv":
        frame.to_csv(path, index=False)
    elif output_format == "jsonl":
        frame.to_json(path, orient="records", lines=True)
    else:
        raise ValueError("output_format must be one of: parquet, csv, jsonl")


def build_projection_qc_summary(frame: pd.DataFrame) -> dict[str, object]:
    """Build lightweight QC metrics for fixture projection output."""
    status_counts = frame["map_status"].value_counts().sort_index().to_dict()
    coverage = frame["coverage_frac"]
    seq_identity = frame["seq_identity"]

    return {
        "row_count": int(frame.shape[0]),
        "unique_ccre_ids": int(frame["ccre_id"].nunique()),
        "unique_haplotype_ids": int(frame["haplotype_id"].nunique()),
        "map_status_counts": {str(k): int(v) for k, v in status_counts.items()},
        "coverage_frac": {
            "min": float(coverage.min()),
            "mean": float(coverage.mean()),
            "max": float(coverage.max()),
        },
        "seq_identity": {
            "min": float(seq_identity.min()),
            "mean": float(seq_identity.mean()),
            "max": float(seq_identity.max()),
        },
    }


def write_projection_qc_summary(summary: dict[str, object], path: str | Path) -> Path:
    """Write projection QC summary JSON."""
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return file_path


def project_fixture_haplotypes(
    *,
    ccre_ref_path: str | Path,
    haplotypes_path: str | Path,
    output_path: str | Path,
    qc_summary_path: str | Path,
    output_format: str = "jsonl",
    ccre_ref_format: str | None = None,
) -> ProjectionResult:
    """Produce fixture `hap_projection` rows and QC summary artifacts."""
    frame = build_fixture_hap_projection(
        ccre_ref_path=ccre_ref_path,
        haplotypes_path=haplotypes_path,
        ccre_ref_format=ccre_ref_format,
    )

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    _write_projection_frame(frame, output_file, output_format)

    qc_summary = build_projection_qc_summary(frame)
    qc_path = write_projection_qc_summary(qc_summary, qc_summary_path)

    return ProjectionResult(
        row_count=int(frame.shape[0]),
        output_path=output_file,
        qc_summary_path=qc_path,
        output_format=output_format,
    )
