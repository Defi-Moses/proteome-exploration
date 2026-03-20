"""State calling from hap_projection rows."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Iterator, Mapping, TextIO

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

_DEFAULT_STATE_STREAM_CHUNK_ROWS = 20_000


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


@dataclass
class _StateSummaryAccumulator:
    row_count: int = 0
    state_class_counts: dict[str, int] = field(default_factory=dict)
    qc_flag_counts: dict[str, int] = field(default_factory=dict)

    def update(self, *, state_class: str, qc_flag: str) -> None:
        self.row_count += 1
        self.state_class_counts[state_class] = self.state_class_counts.get(state_class, 0) + 1
        self.qc_flag_counts[qc_flag] = self.qc_flag_counts.get(qc_flag, 0) + 1

    def summary(self) -> dict[str, object]:
        if self.row_count <= 0:
            raise ValueError("ccre_state row_count must be > 0")
        return {
            "row_count": int(self.row_count),
            "state_class_counts": {k: int(self.state_class_counts[k]) for k in sorted(self.state_class_counts)},
            "qc_flag_counts": {k: int(self.qc_flag_counts[k]) for k in sorted(self.qc_flag_counts)},
        }


class _StateRowWriter:
    def __init__(self, *, path: str | Path, output_format: str, chunk_rows: int) -> None:
        self.path = Path(path)
        self.output_format = output_format.lower()
        if self.output_format not in {"parquet", "csv", "jsonl"}:
            raise ValueError("output_format must be one of: parquet, csv, jsonl")

        if chunk_rows <= 0:
            raise ValueError("chunk_rows must be > 0")
        self.chunk_rows = chunk_rows

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._buffer: list[dict[str, object]] = []

        self._json_handle: TextIO | None = None
        self._csv_handle: TextIO | None = None
        self._csv_writer: csv.DictWriter | None = None
        self._parquet_writer = None

        if self.output_format == "jsonl":
            self._json_handle = self.path.open("w", encoding="utf-8")
        elif self.output_format == "csv":
            self._csv_handle = self.path.open("w", encoding="utf-8", newline="")
            self._csv_writer = csv.DictWriter(
                self._csv_handle,
                fieldnames=CCRE_STATE_COLUMNS,
                extrasaction="raise",
            )
            self._csv_writer.writeheader()
        elif self.output_format == "parquet":
            if not _parquet_available():
                raise RuntimeError(
                    "Parquet output requires pyarrow or fastparquet. "
                    "Install one of those engines or choose --output-format csv/jsonl."
                )

    def write_row(self, row: dict[str, object]) -> None:
        self._buffer.append(row)
        if len(self._buffer) >= self.chunk_rows:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return

        if self.output_format == "jsonl":
            assert self._json_handle is not None
            for row in self._buffer:
                self._json_handle.write(json.dumps(row, separators=(",", ":")) + "\n")
        elif self.output_format == "csv":
            assert self._csv_writer is not None
            self._csv_writer.writerows(self._buffer)
        elif self.output_format == "parquet":
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.Table.from_pylist(self._buffer)
            if self._parquet_writer is None:
                self._parquet_writer = pq.ParquetWriter(str(self.path), table.schema)
            self._parquet_writer.write_table(table)

        self._buffer.clear()

    def close(self) -> None:
        self._flush()

        if self._json_handle is not None:
            self._json_handle.close()
            self._json_handle = None
        if self._csv_handle is not None:
            self._csv_handle.close()
            self._csv_handle = None
        if self._parquet_writer is not None:
            self._parquet_writer.close()
            self._parquet_writer = None


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


def _as_str(value: object) -> str:
    return str(value)


def _as_int(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return int(value)
    text = str(value).strip()
    if not text:
        raise ValueError("Expected integer value, got empty string")
    return int(text)


def _as_float(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        raise ValueError("Expected float value, got empty string")
    return float(text)


def _as_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if text in {"", "None", "none", "nan", "NaN", "NULL", "null"}:
        return None
    return int(float(text)) if ("." in text) else int(text)


def _as_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if text in {"", "None", "none", "nan", "NaN", "NULL", "null"}:
        return None
    return text


def _normalize_projection_row(raw: Mapping[str, object]) -> dict[str, object]:
    missing = [column for column in HAP_PROJECTION_COLUMNS if column not in raw]
    if missing:
        raise ValueError(f"hap_projection row missing required columns: {missing}")

    row = {
        "ccre_id": _as_str(raw["ccre_id"]),
        "haplotype_id": _as_str(raw["haplotype_id"]),
        "ref_chr": _as_str(raw["ref_chr"]),
        "ref_start": _as_int(raw["ref_start"]),
        "ref_end": _as_int(raw["ref_end"]),
        "alt_contig": _as_optional_str(raw["alt_contig"]),
        "alt_start": _as_optional_int(raw["alt_start"]),
        "alt_end": _as_optional_int(raw["alt_end"]),
        "orientation": _as_str(raw["orientation"]),
        "map_status": _as_str(raw["map_status"]),
        "coverage_frac": _as_float(raw["coverage_frac"]),
        "seq_identity": _as_float(raw["seq_identity"]),
        "split_count": _as_int(raw["split_count"]),
        "copy_count": _as_int(raw["copy_count"]),
        "flank_synteny_confidence": _as_float(raw["flank_synteny_confidence"]),
        "mapping_method": _as_str(raw["mapping_method"]),
    }

    if row["ref_end"] <= row["ref_start"]:
        raise ValueError("hap_projection row has ref_end <= ref_start")
    return row


def _iter_projection_rows(path: str | Path, *, input_format: str | None = None) -> Iterator[dict[str, object]]:
    file_path = Path(path)
    fmt = (input_format or _infer_format_from_path(file_path)).lower()

    if fmt == "jsonl":
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                if not isinstance(payload, dict):
                    raise ValueError("hap_projection JSONL row must decode to object")
                yield _normalize_projection_row(payload)
        return

    if fmt == "csv":
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != HAP_PROJECTION_COLUMNS:
                raise ValueError(
                    "hap_projection column contract mismatch: "
                    f"expected={HAP_PROJECTION_COLUMNS} actual={reader.fieldnames}"
                )
            for row in reader:
                yield _normalize_projection_row(row)
        return

    if fmt == "parquet":
        frame = read_hap_projection(file_path, input_format="parquet")
        for record in frame.to_dict(orient="records"):
            yield _normalize_projection_row(record)
        return

    raise ValueError("input_format must be one of: parquet, csv, jsonl")


def _call_state(row: Mapping[str, object], thresholds: StateCallThresholds) -> str:
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


def _build_state_row(proj_row: Mapping[str, object], *, thresholds: StateCallThresholds) -> dict[str, object]:
    state_class = _call_state(proj_row, thresholds)
    qc_flag = "ok"
    if state_class == "ambiguous" or float(proj_row["flank_synteny_confidence"]) < thresholds.min_flank_synteny_confidence_ok:
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
        "alt_contig": proj_row["alt_contig"],
        "alt_start": proj_row["alt_start"],
        "alt_end": proj_row["alt_end"],
    }

    state_row = {
        "ccre_id": str(proj_row["ccre_id"]),
        "haplotype_id": str(proj_row["haplotype_id"]),
        "state_class": state_class,
        "state_reason": json.dumps(reason, sort_keys=True),
        "local_sv_class": _local_sv_class(state_class),
        "replacement_candidate_id": None,
        "qc_flag": qc_flag,
    }
    return state_row


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
        row = _normalize_projection_row(proj_row.to_dict())
        rows.append(_build_state_row(row, thresholds=config))

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
    stream_chunk_rows: int = _DEFAULT_STATE_STREAM_CHUNK_ROWS,
) -> StateCallResult:
    config = thresholds or StateCallThresholds()

    out = Path(output_path)
    writer = _StateRowWriter(path=out, output_format=output_format, chunk_rows=stream_chunk_rows)
    summary = _StateSummaryAccumulator()

    try:
        for proj_row in _iter_projection_rows(projection_path, input_format=projection_format):
            state_row = _build_state_row(proj_row, thresholds=config)
            writer.write_row(state_row)
            summary.update(
                state_class=str(state_row["state_class"]),
                qc_flag=str(state_row["qc_flag"]),
            )
    finally:
        writer.close()

    qc_path = write_state_qc_summary(summary.summary(), qc_summary_path)

    return StateCallResult(
        row_count=int(summary.row_count),
        output_path=out,
        qc_summary_path=qc_path,
        output_format=output_format,
    )
