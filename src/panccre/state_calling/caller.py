"""State calling from hap_projection rows."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

import pandas as pd

from panccre.projection import HAP_PROJECTION_COLUMNS

CCRE_STATE_COLUMNS = [
    "ccre_id",
    "haplotype_id",
    "state_class",
    "state_reason",
    "local_sv_class",
    "replacement_candidate_id",
    "qc_flag",
]


@dataclass(frozen=True)
class StateCallThresholds:
    min_coverage_frac_conserved: float = 0.90
    min_identity_conserved: float = 0.97
    max_split_count_conserved: int = 1
    duplicate_copy_threshold: int = 2
    min_flank_synteny_confidence_ok: float = 0.60


@dataclass(frozen=True)
class StateCallResult:
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


def _infer_format_from_path(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return "parquet"
    if suffix == ".csv":
        return "csv"
    if suffix in {".jsonl", ".ndjson"}:
        return "jsonl"
    raise ValueError(f"Could not infer format from extension: {path}")


def read_hap_projection(path: str | Path, *, input_format: str | None = None) -> pd.DataFrame:
    """Read hap_projection rows and validate contract."""
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

    validate_hap_projection_input(frame)
    return frame


def validate_hap_projection_input(frame: pd.DataFrame) -> None:
    actual = list(frame.columns)
    if actual != HAP_PROJECTION_COLUMNS:
        raise ValueError(
            "hap_projection column contract mismatch: "
            f"expected={HAP_PROJECTION_COLUMNS} actual={actual}"
        )
    if frame.empty:
        raise ValueError("hap_projection frame must not be empty")


def _local_sv_class(state_class: str) -> str:
    if state_class == "absent":
        return "deletion_like"
    if state_class == "fractured":
        return "breakpoint"
    if state_class == "duplicated":
        return "duplication"
    if state_class == "diverged":
        return "sequence_divergence"
    if state_class == "ambiguous":
        return "unknown"
    return "none"


def _call_state(row: pd.Series, thresholds: StateCallThresholds) -> str:
    map_status = str(row["map_status"])
    coverage_frac = float(row["coverage_frac"])
    seq_identity = float(row["seq_identity"])
    split_count = int(row["split_count"])
    copy_count = int(row["copy_count"])

    if map_status == "exact":
        if (
            coverage_frac >= thresholds.min_coverage_frac_conserved
            and seq_identity >= thresholds.min_identity_conserved
            and split_count <= thresholds.max_split_count_conserved
        ):
            return "conserved"
        return "diverged"

    if map_status == "diverged":
        return "diverged"
    if map_status == "fractured":
        return "fractured"
    if map_status == "absent":
        return "absent"
    if map_status == "duplicated":
        return "duplicated" if copy_count >= thresholds.duplicate_copy_threshold else "diverged"
    return "ambiguous"


def build_ccre_state(
    projection: pd.DataFrame,
    *,
    thresholds: StateCallThresholds | None = None,
) -> pd.DataFrame:
    """Convert projection rows into ccre_state rows."""
    validate_hap_projection_input(projection)
    config = thresholds or StateCallThresholds()

    rows: list[dict[str, object]] = []
    for _, proj_row in projection.iterrows():
        state_class = _call_state(proj_row, config)
        qc_flag = "ok"
        if state_class == "ambiguous" or float(proj_row["flank_synteny_confidence"]) < config.min_flank_synteny_confidence_ok:
            qc_flag = "needs_review"

        reason = {
            "map_status": str(proj_row["map_status"]),
            "coverage_frac": float(proj_row["coverage_frac"]),
            "seq_identity": float(proj_row["seq_identity"]),
            "split_count": int(proj_row["split_count"]),
            "copy_count": int(proj_row["copy_count"]),
            "flank_synteny_confidence": float(proj_row["flank_synteny_confidence"]),
            "ref_chr": str(proj_row["ref_chr"]),
            "ref_start": int(proj_row["ref_start"]),
            "ref_end": int(proj_row["ref_end"]),
            "alt_contig": None if pd.isna(proj_row["alt_contig"]) else str(proj_row["alt_contig"]),
            "alt_start": None if pd.isna(proj_row["alt_start"]) else int(proj_row["alt_start"]),
            "alt_end": None if pd.isna(proj_row["alt_end"]) else int(proj_row["alt_end"]),
        }

        rows.append(
            {
                "ccre_id": str(proj_row["ccre_id"]),
                "haplotype_id": str(proj_row["haplotype_id"]),
                "state_class": state_class,
                "state_reason": json.dumps(reason, sort_keys=True),
                "local_sv_class": _local_sv_class(state_class),
                "replacement_candidate_id": None,
                "qc_flag": qc_flag,
            }
        )

    state = pd.DataFrame(rows, columns=CCRE_STATE_COLUMNS)
    validate_ccre_state_frame(state)
    return state


def validate_ccre_state_frame(frame: pd.DataFrame) -> None:
    actual = list(frame.columns)
    if actual != CCRE_STATE_COLUMNS:
        raise ValueError(f"ccre_state column contract mismatch: expected={CCRE_STATE_COLUMNS} actual={actual}")
    if frame.empty:
        raise ValueError("ccre_state frame must not be empty")
    if frame[["ccre_id", "haplotype_id", "state_class"]].isna().any().any():
        raise ValueError("ccre_state has null key fields")
    if frame.duplicated(subset=["ccre_id", "haplotype_id"]).any():
        raise ValueError("ccre_state has duplicate (ccre_id, haplotype_id) rows")


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


def build_state_qc_summary(frame: pd.DataFrame) -> dict[str, object]:
    counts = frame["state_class"].value_counts().sort_index().to_dict()
    qc_counts = frame["qc_flag"].value_counts().sort_index().to_dict()
    return {
        "row_count": int(frame.shape[0]),
        "state_class_counts": {str(k): int(v) for k, v in counts.items()},
        "qc_flag_counts": {str(k): int(v) for k, v in qc_counts.items()},
    }


def write_state_qc_summary(summary: dict[str, object], path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out


def call_states_from_projection(
    *,
    projection_path: str | Path,
    output_path: str | Path,
    qc_summary_path: str | Path,
    output_format: str = "jsonl",
    projection_format: str | None = None,
    thresholds: StateCallThresholds | None = None,
) -> StateCallResult:
    projection = read_hap_projection(projection_path, input_format=projection_format)
    state = build_ccre_state(projection, thresholds=thresholds)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    _write_frame(state, out, output_format)

    summary = build_state_qc_summary(state)
    qc_path = write_state_qc_summary(summary, qc_summary_path)

    return StateCallResult(
        row_count=int(state.shape[0]),
        output_path=out,
        qc_summary_path=qc_path,
        output_format=output_format,
    )
