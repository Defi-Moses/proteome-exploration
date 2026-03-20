"""Replacement candidate discovery from cCRE state rows."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
from typing import Iterator, Mapping, TextIO

import pandas as pd

from panccre.state_calling import CCRE_STATE_COLUMNS

REPLACEMENT_CANDIDATE_COLUMNS = [
    "candidate_id",
    "parent_ccre_id",
    "haplotype_id",
    "window_class",
    "alt_contig",
    "alt_start",
    "alt_end",
    "seq_len",
    "repeat_class",
    "te_family",
    "motif_count",
    "gc_content",
    "nearest_gene",
    "nearest_gene_distance",
]

_REPEAT_CLASSES = ["LINE", "SINE", "LTR", "DNA", "low_complexity"]
_TE_FAMILIES = ["L1", "Alu", "ERV", "hAT", "simple"]
_DEFAULT_CANDIDATE_STREAM_CHUNK_ROWS = 20_000


@dataclass(frozen=True)
class CandidateDiscoveryResult:
    row_count: int
    output_path: Path
    qc_summary_path: Path
    output_format: str


@dataclass
class _CandidateSummaryAccumulator:
    row_count: int = 0
    window_class_counts: dict[str, int] = field(default_factory=dict)
    repeat_class_counts: dict[str, int] = field(default_factory=dict)
    seq_len_sum: int = 0
    seq_len_min: int | None = None
    seq_len_max: int | None = None

    def update(self, row: Mapping[str, object]) -> None:
        self.row_count += 1
        window_class = str(row["window_class"])
        repeat_class = str(row["repeat_class"])
        seq_len = int(row["seq_len"])

        self.window_class_counts[window_class] = self.window_class_counts.get(window_class, 0) + 1
        self.repeat_class_counts[repeat_class] = self.repeat_class_counts.get(repeat_class, 0) + 1

        self.seq_len_sum += seq_len
        if self.seq_len_min is None or seq_len < self.seq_len_min:
            self.seq_len_min = seq_len
        if self.seq_len_max is None or seq_len > self.seq_len_max:
            self.seq_len_max = seq_len

    def summary(self) -> dict[str, object]:
        if self.row_count <= 0:
            raise ValueError("replacement_candidate frame must not be empty")
        return {
            "row_count": int(self.row_count),
            "window_class_counts": {k: int(self.window_class_counts[k]) for k in sorted(self.window_class_counts)},
            "repeat_class_counts": {k: int(self.repeat_class_counts[k]) for k in sorted(self.repeat_class_counts)},
            "seq_len": {
                "min": int(self.seq_len_min if self.seq_len_min is not None else 0),
                "mean": float(self.seq_len_sum / self.row_count),
                "max": int(self.seq_len_max if self.seq_len_max is not None else 0),
            },
        }


class _CandidateRowWriter:
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
                fieldnames=REPLACEMENT_CANDIDATE_COLUMNS,
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


def read_ccre_state(path: str | Path, *, input_format: str | None = None) -> pd.DataFrame:
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
    if actual != CCRE_STATE_COLUMNS:
        raise ValueError(f"ccre_state column contract mismatch: expected={CCRE_STATE_COLUMNS} actual={actual}")
    if frame.empty:
        raise ValueError("ccre_state frame must not be empty")
    return frame


def _stable_int(value: str, modulo: int) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:10], 16) % modulo


def _parse_state_reason(payload: str) -> dict[str, object]:
    obj = json.loads(payload)
    if not isinstance(obj, dict):
        raise ValueError("state_reason must decode to object")
    return obj


def _as_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if text in {"", "None", "none", "nan", "NaN", "NULL", "null"}:
        return None
    return int(float(text)) if "." in text else int(text)


def _normalize_state_row(raw: Mapping[str, object]) -> dict[str, object]:
    missing = [column for column in CCRE_STATE_COLUMNS if column not in raw]
    if missing:
        raise ValueError(f"ccre_state row missing required columns: {missing}")

    return {
        "ccre_id": str(raw["ccre_id"]),
        "haplotype_id": str(raw["haplotype_id"]),
        "state_class": str(raw["state_class"]),
        "state_reason": str(raw["state_reason"]),
        "local_sv_class": str(raw["local_sv_class"]),
        "replacement_candidate_id": raw["replacement_candidate_id"],
        "qc_flag": str(raw["qc_flag"]),
    }


def _iter_state_rows(path: str | Path, *, input_format: str | None = None) -> Iterator[dict[str, object]]:
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
                    raise ValueError("ccre_state JSONL row must decode to object")
                yield _normalize_state_row(payload)
        return

    if fmt == "csv":
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != CCRE_STATE_COLUMNS:
                raise ValueError(
                    "ccre_state column contract mismatch: "
                    f"expected={CCRE_STATE_COLUMNS} actual={reader.fieldnames}"
                )
            for row in reader:
                yield _normalize_state_row(row)
        return

    if fmt == "parquet":
        frame = read_ccre_state(file_path, input_format="parquet")
        for record in frame.to_dict(orient="records"):
            yield _normalize_state_row(record)
        return

    raise ValueError("input_format must be one of: parquet, csv, jsonl")


def _candidate_from_state_row(row: Mapping[str, object]) -> dict[str, object] | None:
    state_class = str(row["state_class"])
    if state_class not in {"absent", "fractured", "duplicated"}:
        return None

    reason = _parse_state_reason(str(row["state_reason"]))
    parent_ccre_id = str(row["ccre_id"])
    haplotype_id = str(row["haplotype_id"])
    token = f"{parent_ccre_id}|{haplotype_id}|{state_class}"

    ref_chr = str(reason.get("ref_chr"))
    ref_start = int(reason.get("ref_start"))
    ref_end = int(reason.get("ref_end"))

    alt_contig_value = reason.get("alt_contig")
    alt_contig = ref_chr if alt_contig_value is None else str(alt_contig_value)

    if state_class == "absent":
        window_class = "absent_window"
        alt_start = ref_start - 100
        alt_end = ref_end + 100
    elif state_class == "fractured":
        window_class = "fracture_gap"
        alt_start = _as_optional_int(reason.get("alt_start"))
        alt_end = _as_optional_int(reason.get("alt_end"))
        alt_start = ref_start if alt_start is None else alt_start
        alt_end = (ref_start + (ref_end - ref_start) // 2) if alt_end is None else alt_end
    else:
        window_class = "duplicate_neighbor"
        alt_start = _as_optional_int(reason.get("alt_start"))
        alt_end = _as_optional_int(reason.get("alt_end"))
        alt_start = (ref_start - 50) if alt_start is None else alt_start
        alt_end = (ref_end - 50) if alt_end is None else alt_end

    if alt_end <= alt_start:
        alt_end = alt_start + 50

    seq_len = alt_end - alt_start
    repeat_class = _REPEAT_CLASSES[_stable_int(token + "|repeat", len(_REPEAT_CLASSES))]
    te_family = _TE_FAMILIES[_stable_int(token + "|te", len(_TE_FAMILIES))]
    motif_count = 1 + _stable_int(token + "|motif", 15)
    gc_content = round(0.30 + (_stable_int(token + "|gc", 35) / 100), 3)
    nearest_gene_distance = 100 + _stable_int(token + "|dist", 40000)
    nearest_gene = f"GENE{1000 + _stable_int(token + '|gene', 9000)}"

    candidate_id = f"cand_{parent_ccre_id}_{haplotype_id}".replace("|", "_")

    return {
        "candidate_id": candidate_id,
        "parent_ccre_id": parent_ccre_id,
        "haplotype_id": haplotype_id,
        "window_class": window_class,
        "alt_contig": alt_contig,
        "alt_start": int(alt_start),
        "alt_end": int(alt_end),
        "seq_len": int(seq_len),
        "repeat_class": repeat_class,
        "te_family": te_family,
        "motif_count": int(motif_count),
        "gc_content": float(gc_content),
        "nearest_gene": nearest_gene,
        "nearest_gene_distance": int(nearest_gene_distance),
    }


def discover_replacement_candidates(state: pd.DataFrame) -> pd.DataFrame:
    """Generate deterministic replacement candidates for altered states."""
    candidates: list[dict[str, object]] = []

    for _, row in state.iterrows():
        candidate = _candidate_from_state_row(_normalize_state_row(row.to_dict()))
        if candidate is not None:
            candidates.append(candidate)

    frame = pd.DataFrame(candidates, columns=REPLACEMENT_CANDIDATE_COLUMNS)
    validate_replacement_candidates(frame)
    return frame


def validate_replacement_candidates(frame: pd.DataFrame) -> None:
    actual = list(frame.columns)
    if actual != REPLACEMENT_CANDIDATE_COLUMNS:
        raise ValueError(
            "replacement_candidate column contract mismatch: "
            f"expected={REPLACEMENT_CANDIDATE_COLUMNS} actual={actual}"
        )
    if frame.empty:
        raise ValueError("replacement_candidate frame must not be empty")
    if frame["candidate_id"].duplicated().any():
        raise ValueError("replacement_candidate contains duplicate candidate_id values")
    if (frame["alt_end"] <= frame["alt_start"]).any():
        raise ValueError("replacement_candidate contains rows where alt_end <= alt_start")


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


def build_candidate_qc_summary(frame: pd.DataFrame) -> dict[str, object]:
    window_counts = frame["window_class"].value_counts().sort_index().to_dict()
    repeat_counts = frame["repeat_class"].value_counts().sort_index().to_dict()
    return {
        "row_count": int(frame.shape[0]),
        "window_class_counts": {str(k): int(v) for k, v in window_counts.items()},
        "repeat_class_counts": {str(k): int(v) for k, v in repeat_counts.items()},
        "seq_len": {
            "min": int(frame["seq_len"].min()),
            "mean": float(frame["seq_len"].mean()),
            "max": int(frame["seq_len"].max()),
        },
    }


def write_candidate_qc_summary(summary: dict[str, object], path: str | Path) -> Path:
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out


def run_candidate_discovery(
    *,
    ccre_state_path: str | Path,
    output_path: str | Path,
    qc_summary_path: str | Path,
    output_format: str = "jsonl",
    ccre_state_format: str | None = None,
    stream_chunk_rows: int = _DEFAULT_CANDIDATE_STREAM_CHUNK_ROWS,
) -> CandidateDiscoveryResult:
    writer = _CandidateRowWriter(path=output_path, output_format=output_format, chunk_rows=stream_chunk_rows)
    summary = _CandidateSummaryAccumulator()
    seen_candidate_ids: set[str] = set()

    try:
        for state_row in _iter_state_rows(ccre_state_path, input_format=ccre_state_format):
            candidate_row = _candidate_from_state_row(state_row)
            if candidate_row is None:
                continue
            candidate_id = str(candidate_row["candidate_id"])
            if candidate_id in seen_candidate_ids:
                raise ValueError("replacement_candidate contains duplicate candidate_id values")
            seen_candidate_ids.add(candidate_id)

            writer.write_row(candidate_row)
            summary.update(candidate_row)
    finally:
        writer.close()

    qc = write_candidate_qc_summary(summary.summary(), qc_summary_path)

    return CandidateDiscoveryResult(
        row_count=int(summary.row_count),
        output_path=Path(output_path),
        qc_summary_path=qc,
        output_format=output_format,
    )
