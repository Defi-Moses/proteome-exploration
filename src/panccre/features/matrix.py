"""Feature matrix construction for state and replacement entities."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

import pandas as pd

from panccre.candidate_discovery import REPLACEMENT_CANDIDATE_COLUMNS
from panccre.state_calling import CCRE_STATE_COLUMNS

FEATURE_MATRIX_COLUMNS = [
    "entity_id",
    "entity_type",
    "feature_name",
    "feature_value",
    "feature_version",
]


@dataclass(frozen=True)
class FeatureBuildResult:
    row_count: int
    output_path: Path
    output_format: str


def _parquet_available() -> bool:
    try:
        pd.io.parquet.get_engine("auto")
        return True
    except Exception:
        return False


def _infer_format_from_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return "parquet"
    if suffix == ".csv":
        return "csv"
    if suffix in {".jsonl", ".ndjson"}:
        return "jsonl"
    raise ValueError(f"Could not infer format from extension: {path}")


def _read_table(path: str | Path, expected_columns: list[str], input_format: str | None = None) -> pd.DataFrame:
    file_path = Path(path)
    fmt = (input_format or _infer_format_from_path(file_path)).lower()

    if fmt == "parquet":
        if not _parquet_available():
            raise RuntimeError("Reading parquet requires pyarrow or fastparquet")
        frame = pd.read_parquet(file_path)
    elif fmt == "csv":
        frame = pd.read_csv(file_path)
    elif fmt == "jsonl":
        frame = pd.read_json(file_path, lines=True)
    else:
        raise ValueError("input_format must be one of: parquet, csv, jsonl")

    actual = list(frame.columns)
    if actual != expected_columns:
        raise ValueError(f"column contract mismatch: expected={expected_columns} actual={actual}")
    if frame.empty:
        raise ValueError(f"Input table is empty: {file_path}")
    return frame


def _state_features(state_row: pd.Series, feature_version: str) -> list[dict[str, object]]:
    reason = json.loads(str(state_row["state_reason"]))
    entity_id = f"{state_row['ccre_id']}|{state_row['haplotype_id']}"
    state_class = str(state_row["state_class"])

    features = [
        {"entity_id": entity_id, "entity_type": "ref_state", "feature_name": "coverage_frac", "feature_value": float(reason.get("coverage_frac", 0.0)), "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "ref_state", "feature_name": "seq_identity", "feature_value": float(reason.get("seq_identity", 0.0)), "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "ref_state", "feature_name": "split_count", "feature_value": float(reason.get("split_count", 0)), "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "ref_state", "feature_name": "copy_count", "feature_value": float(reason.get("copy_count", 0)), "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "ref_state", "feature_name": "flank_synteny_confidence", "feature_value": float(reason.get("flank_synteny_confidence", 0.0)), "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "ref_state", "feature_name": "state_is_absent", "feature_value": 1.0 if state_class == "absent" else 0.0, "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "ref_state", "feature_name": "state_is_fractured", "feature_value": 1.0 if state_class == "fractured" else 0.0, "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "ref_state", "feature_name": "state_is_diverged", "feature_value": 1.0 if state_class == "diverged" else 0.0, "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "ref_state", "feature_name": "state_is_duplicated", "feature_value": 1.0 if state_class == "duplicated" else 0.0, "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "ref_state", "feature_name": "state_is_ambiguous", "feature_value": 1.0 if state_class == "ambiguous" else 0.0, "feature_version": feature_version},
    ]
    return features


def _candidate_features(candidate_row: pd.Series, feature_version: str) -> list[dict[str, object]]:
    entity_id = str(candidate_row["candidate_id"])
    repeat_class = str(candidate_row["repeat_class"])

    features = [
        {"entity_id": entity_id, "entity_type": "replacement_candidate", "feature_name": "seq_len", "feature_value": float(candidate_row["seq_len"]), "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "replacement_candidate", "feature_name": "gc_content", "feature_value": float(candidate_row["gc_content"]), "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "replacement_candidate", "feature_name": "motif_count", "feature_value": float(candidate_row["motif_count"]), "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "replacement_candidate", "feature_name": "nearest_gene_distance", "feature_value": float(candidate_row["nearest_gene_distance"]), "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "replacement_candidate", "feature_name": "is_repeat_LINE", "feature_value": 1.0 if repeat_class == "LINE" else 0.0, "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "replacement_candidate", "feature_name": "is_repeat_SINE", "feature_value": 1.0 if repeat_class == "SINE" else 0.0, "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "replacement_candidate", "feature_name": "is_repeat_LTR", "feature_value": 1.0 if repeat_class == "LTR" else 0.0, "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "replacement_candidate", "feature_name": "is_repeat_DNA", "feature_value": 1.0 if repeat_class == "DNA" else 0.0, "feature_version": feature_version},
        {"entity_id": entity_id, "entity_type": "replacement_candidate", "feature_name": "is_repeat_low_complexity", "feature_value": 1.0 if repeat_class == "low_complexity" else 0.0, "feature_version": feature_version},
    ]
    return features


def build_feature_matrix(
    state: pd.DataFrame,
    candidates: pd.DataFrame,
    *,
    feature_version: str = "v1",
) -> pd.DataFrame:
    """Build tall feature matrix for state and candidate entities."""
    state_actual = list(state.columns)
    if state_actual != CCRE_STATE_COLUMNS:
        raise ValueError(f"ccre_state column contract mismatch: expected={CCRE_STATE_COLUMNS} actual={state_actual}")

    candidate_actual = list(candidates.columns)
    if candidate_actual != REPLACEMENT_CANDIDATE_COLUMNS:
        raise ValueError(
            "replacement_candidate column contract mismatch: "
            f"expected={REPLACEMENT_CANDIDATE_COLUMNS} actual={candidate_actual}"
        )

    rows: list[dict[str, object]] = []
    for _, row in state.iterrows():
        rows.extend(_state_features(row, feature_version))
    for _, row in candidates.iterrows():
        rows.extend(_candidate_features(row, feature_version))

    feature_matrix = pd.DataFrame(rows, columns=FEATURE_MATRIX_COLUMNS)
    validate_feature_matrix(feature_matrix)
    return feature_matrix


def validate_feature_matrix(frame: pd.DataFrame) -> None:
    actual = list(frame.columns)
    if actual != FEATURE_MATRIX_COLUMNS:
        raise ValueError(f"feature_matrix column contract mismatch: expected={FEATURE_MATRIX_COLUMNS} actual={actual}")
    if frame.empty:
        raise ValueError("feature_matrix must not be empty")
    if frame.duplicated(subset=["entity_id", "entity_type", "feature_name", "feature_version"]).any():
        raise ValueError("feature_matrix contains duplicate rows")


def _write_frame(frame: pd.DataFrame, path: Path, output_format: str) -> None:
    fmt = output_format.lower()
    if fmt == "parquet":
        if not _parquet_available():
            raise RuntimeError(
                "Parquet output requires pyarrow or fastparquet. "
                "Install one of those engines or choose --output-format csv/jsonl."
            )
        frame.to_parquet(path, index=False)
    elif fmt == "csv":
        frame.to_csv(path, index=False)
    elif fmt == "jsonl":
        frame.to_json(path, orient="records", lines=True)
    else:
        raise ValueError("output_format must be one of: parquet, csv, jsonl")


def run_feature_build(
    *,
    ccre_state_path: str | Path,
    replacement_candidate_path: str | Path,
    output_path: str | Path,
    output_format: str = "jsonl",
    ccre_state_format: str | None = None,
    replacement_candidate_format: str | None = None,
    feature_version: str = "v1",
) -> FeatureBuildResult:
    state = _read_table(ccre_state_path, CCRE_STATE_COLUMNS, input_format=ccre_state_format)
    candidates = _read_table(
        replacement_candidate_path,
        REPLACEMENT_CANDIDATE_COLUMNS,
        input_format=replacement_candidate_format,
    )
    matrix = build_feature_matrix(state, candidates, feature_version=feature_version)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    _write_frame(matrix, out, output_format)

    return FeatureBuildResult(
        row_count=int(matrix.shape[0]),
        output_path=out,
        output_format=output_format,
    )
