"""Feature matrix construction for state and replacement entities."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterator, Mapping, TextIO

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

_DEFAULT_FEATURE_STREAM_CHUNK_ROWS = 20_000


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


class _FeatureRowWriter:
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
                fieldnames=FEATURE_MATRIX_COLUMNS,
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


def _normalize_state_row(raw: Mapping[str, object]) -> dict[str, object]:
    missing = [column for column in CCRE_STATE_COLUMNS if column not in raw]
    if missing:
        raise ValueError(f"ccre_state row missing required columns: {missing}")
    return {
        "ccre_id": str(raw["ccre_id"]),
        "haplotype_id": str(raw["haplotype_id"]),
        "state_class": str(raw["state_class"]),
        "state_reason": str(raw["state_reason"]),
    }


def _normalize_candidate_row(raw: Mapping[str, object]) -> dict[str, object]:
    missing = [column for column in REPLACEMENT_CANDIDATE_COLUMNS if column not in raw]
    if missing:
        raise ValueError(f"replacement_candidate row missing required columns: {missing}")
    return {
        "candidate_id": str(raw["candidate_id"]),
        "repeat_class": str(raw["repeat_class"]),
        "seq_len": float(raw["seq_len"]),
        "gc_content": float(raw["gc_content"]),
        "motif_count": float(raw["motif_count"]),
        "nearest_gene_distance": float(raw["nearest_gene_distance"]),
    }


def _iter_table_rows(
    path: str | Path,
    expected_columns: list[str],
    *,
    input_format: str | None = None,
) -> Iterator[dict[str, object]]:
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
                    raise ValueError("JSONL row must decode to object")
                actual = list(payload.keys())
                if actual != expected_columns:
                    raise ValueError(
                        f"column contract mismatch: expected={expected_columns} actual={actual}"
                    )
                yield payload
        return

    if fmt == "csv":
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != expected_columns:
                raise ValueError(
                    f"column contract mismatch: expected={expected_columns} actual={reader.fieldnames}"
                )
            for row in reader:
                yield row
        return

    if fmt == "parquet":
        frame = _read_table(file_path, expected_columns, input_format="parquet")
        for record in frame.to_dict(orient="records"):
            yield record
        return

    raise ValueError("input_format must be one of: parquet, csv, jsonl")


def _state_features(state_row: Mapping[str, object], feature_version: str) -> list[dict[str, object]]:
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


def _candidate_features(candidate_row: Mapping[str, object], feature_version: str) -> list[dict[str, object]]:
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
    stream_chunk_rows: int = _DEFAULT_FEATURE_STREAM_CHUNK_ROWS,
) -> FeatureBuildResult:
    out = Path(output_path)
    writer = _FeatureRowWriter(path=out, output_format=output_format, chunk_rows=stream_chunk_rows)
    row_count = 0

    try:
        state_seen = False
        for raw_row in _iter_table_rows(
            ccre_state_path,
            CCRE_STATE_COLUMNS,
            input_format=ccre_state_format,
        ):
            state_seen = True
            state_row = _normalize_state_row(raw_row)
            for feature_row in _state_features(state_row, feature_version):
                writer.write_row(feature_row)
                row_count += 1
        if not state_seen:
            raise ValueError(f"Input table is empty: {Path(ccre_state_path)}")

        candidate_seen = False
        for raw_row in _iter_table_rows(
            replacement_candidate_path,
            REPLACEMENT_CANDIDATE_COLUMNS,
            input_format=replacement_candidate_format,
        ):
            candidate_seen = True
            candidate_row = _normalize_candidate_row(raw_row)
            for feature_row in _candidate_features(candidate_row, feature_version):
                writer.write_row(feature_row)
                row_count += 1
        if not candidate_seen:
            raise ValueError(f"Input table is empty: {Path(replacement_candidate_path)}")
    finally:
        writer.close()

    if row_count <= 0:
        raise ValueError("feature_matrix must not be empty")

    return FeatureBuildResult(row_count=int(row_count), output_path=out, output_format=output_format)
