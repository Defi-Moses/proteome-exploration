"""Ingest ENCODE cCRE BED-like files into canonical `ccre_ref` rows."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import re
from typing import Iterable

import pandas as pd

CCRE_REF_COLUMNS = [
    "ccre_id",
    "chr",
    "start",
    "end",
    "strand",
    "ccre_class",
    "biosample_count",
    "context_group",
    "anchor_width",
    "source_release",
]

_ALLOWED_STRANDS = {"+", "-", "."}
_CHROM_RE = re.compile(r"^chr([1-9]|1[0-9]|2[0-2]|X|Y|M|MT)$")


@dataclass(frozen=True)
class CCRERefRow:
    """Canonical cCRE reference table row."""

    ccre_id: str
    chr: str
    start: int
    end: int
    strand: str
    ccre_class: str
    biosample_count: int
    context_group: str
    anchor_width: int
    source_release: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class IngestResult:
    """Summary of an ingest run."""

    row_count: int
    output_path: Path
    output_format: str


def _validate_chromosome(chrom: str, line_context: str) -> None:
    if not _CHROM_RE.match(chrom):
        raise ValueError(f"{line_context} invalid chromosome value: {chrom}")


def _validate_strand(strand: str, line_context: str) -> None:
    if strand not in _ALLOWED_STRANDS:
        raise ValueError(f"{line_context} invalid strand value: {strand}")


def parse_ccre_bed(
    bed_path: str | Path,
    *,
    context_group: str,
    source_release: str,
) -> list[CCRERefRow]:
    """Parse BED-like rows into canonical cCRE reference rows.

    Required columns:
    1. chr
    2. start
    3. end
    4. ccre_id

    Optional columns:
    6. strand
    7. ccre_class
    8. biosample_count
    """
    if not context_group.strip():
        raise ValueError("context_group must be a non-empty string")
    if not source_release.strip():
        raise ValueError("source_release must be a non-empty string")

    path = Path(bed_path)
    rows: list[CCRERefRow] = []
    seen_ccre_ids: set[str] = set()

    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            fields = line.split("\t")
            line_context = f"{path}:{line_number}"
            if len(fields) < 4:
                raise ValueError(f"{line_context} expected at least 4 tab-delimited columns")

            chrom = fields[0].strip()
            _validate_chromosome(chrom, line_context)

            try:
                start = int(fields[1])
                end = int(fields[2])
            except ValueError as exc:
                raise ValueError(f"{line_context} start/end must be integers") from exc

            if start < 0:
                raise ValueError(f"{line_context} start must be >= 0")
            if end <= start:
                raise ValueError(f"{line_context} end must be greater than start")

            ccre_id = fields[3].strip()
            if not ccre_id:
                raise ValueError(f"{line_context} ccre_id must be non-empty")
            if ccre_id in seen_ccre_ids:
                raise ValueError(f"{line_context} duplicate ccre_id detected: {ccre_id}")
            seen_ccre_ids.add(ccre_id)

            strand = fields[5].strip() if len(fields) >= 6 and fields[5].strip() else "."
            _validate_strand(strand, line_context)

            ccre_class = fields[6].strip() if len(fields) >= 7 and fields[6].strip() else "unknown"

            biosample_count = 0
            if len(fields) >= 8 and fields[7].strip():
                try:
                    biosample_count = int(fields[7])
                except ValueError as exc:
                    raise ValueError(f"{line_context} biosample_count must be an integer") from exc
            if biosample_count < 0:
                raise ValueError(f"{line_context} biosample_count must be >= 0")

            rows.append(
                CCRERefRow(
                    ccre_id=ccre_id,
                    chr=chrom,
                    start=start,
                    end=end,
                    strand=strand,
                    ccre_class=ccre_class,
                    biosample_count=biosample_count,
                    context_group=context_group,
                    anchor_width=end - start,
                    source_release=source_release,
                )
            )

    if not rows:
        raise ValueError(f"No data rows found in {path}")

    return rows


def _records_to_dataframe(records: Iterable[CCRERefRow]) -> pd.DataFrame:
    rows = [record.to_dict() for record in records]
    frame = pd.DataFrame(rows, columns=CCRE_REF_COLUMNS)
    validate_ccre_ref_frame(frame)
    return frame


def validate_ccre_ref_frame(frame: pd.DataFrame) -> None:
    """Strict contract guard for `ccre_ref` table shape and values."""
    actual_columns = list(frame.columns)
    if actual_columns != CCRE_REF_COLUMNS:
        raise ValueError(
            "ccre_ref column contract mismatch: "
            f"expected={CCRE_REF_COLUMNS} actual={actual_columns}"
        )

    if frame.empty:
        raise ValueError("ccre_ref frame must not be empty")

    if frame["ccre_id"].isna().any() or (frame["ccre_id"].astype(str).str.strip() == "").any():
        raise ValueError("ccre_ref contains empty ccre_id values")

    if frame["ccre_id"].duplicated().any():
        raise ValueError("ccre_ref contains duplicate ccre_id values")

    for column in ["start", "end", "biosample_count", "anchor_width"]:
        if frame[column].isna().any():
            raise ValueError(f"ccre_ref contains null values in {column}")

    if (frame["start"] < 0).any():
        raise ValueError("ccre_ref contains start values < 0")
    if (frame["end"] <= frame["start"]).any():
        raise ValueError("ccre_ref contains rows where end <= start")
    if (frame["biosample_count"] < 0).any():
        raise ValueError("ccre_ref contains biosample_count < 0")

    expected_anchor_width = frame["end"] - frame["start"]
    if (frame["anchor_width"] != expected_anchor_width).any():
        raise ValueError("ccre_ref contains invalid anchor_width values")

    invalid_chrom = ~frame["chr"].astype(str).str.match(_CHROM_RE)
    if invalid_chrom.any():
        bad = frame.loc[invalid_chrom, "chr"].iloc[0]
        raise ValueError(f"ccre_ref contains invalid chromosome value: {bad}")

    invalid_strand = ~frame["strand"].isin(_ALLOWED_STRANDS)
    if invalid_strand.any():
        bad = frame.loc[invalid_strand, "strand"].iloc[0]
        raise ValueError(f"ccre_ref contains invalid strand value: {bad}")


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
    raise ValueError(f"Could not infer ccre_ref format from extension: {path}")


def read_ccre_ref(path: str | Path, *, input_format: str | None = None) -> pd.DataFrame:
    """Read and validate a cCRE reference table from parquet/csv/jsonl."""
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

    validate_ccre_ref_frame(frame)
    return frame


def write_ccre_ref(
    records: Iterable[CCRERefRow],
    output_path: str | Path,
    *,
    output_format: str,
) -> Path:
    """Write canonical cCRE rows to parquet/csv/jsonl."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    output_format = output_format.lower()
    if output_format not in {"parquet", "csv", "jsonl"}:
        raise ValueError("output_format must be one of: parquet, csv, jsonl")

    frame = _records_to_dataframe(records)

    if output_format == "parquet":
        if not _parquet_available():
            raise RuntimeError(
                "Parquet output requires pyarrow or fastparquet. "
                "Install one of those engines or choose --output-format csv/jsonl."
            )
        frame.to_parquet(path, index=False)
    elif output_format == "csv":
        frame.to_csv(path, index=False)
    else:
        frame.to_json(path, orient="records", lines=True)

    return path


def ingest_ccre_ref(
    *,
    bed_path: str | Path,
    output_path: str | Path,
    context_group: str,
    source_release: str,
    output_format: str = "parquet",
) -> IngestResult:
    """Parse cCRE BED input and materialize the canonical `ccre_ref` table."""
    records = parse_ccre_bed(
        bed_path,
        context_group=context_group,
        source_release=source_release,
    )
    written_path = write_ccre_ref(records, output_path, output_format=output_format)
    return IngestResult(row_count=len(records), output_path=written_path, output_format=output_format)
