"""Ingest ENCODE cCRE BED-like files into canonical ccre_ref rows."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
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


def parse_ccre_bed(
    bed_path: str | Path,
    *,
    context_group: str,
    source_release: str,
) -> list[CCRERefRow]:
    """Parse BED-like rows into canonical cCRE reference rows.

    Expected columns:
    1. chr
    2. start
    3. end
    4. ccre_id
    Optional:
    6. strand
    7. ccre_class
    8. biosample_count
    """
    path = Path(bed_path)
    rows: list[CCRERefRow] = []

    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            fields = line.split("\t")
            if len(fields) < 4:
                raise ValueError(f"{path}:{line_number} expected at least 4 tab-delimited columns")

            chrom = fields[0]
            try:
                start = int(fields[1])
                end = int(fields[2])
            except ValueError as exc:
                raise ValueError(f"{path}:{line_number} start/end must be integers") from exc

            if end <= start:
                raise ValueError(f"{path}:{line_number} end must be greater than start")

            ccre_id = fields[3]
            strand = fields[5] if len(fields) >= 6 and fields[5] else "."
            ccre_class = fields[6] if len(fields) >= 7 and fields[6] else "unknown"

            biosample_count = 0
            if len(fields) >= 8 and fields[7]:
                try:
                    biosample_count = int(fields[7])
                except ValueError as exc:
                    raise ValueError(f"{path}:{line_number} biosample_count must be an integer") from exc

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

    return rows


def _records_to_dataframe(records: Iterable[CCRERefRow]) -> pd.DataFrame:
    rows = [record.to_dict() for record in records]
    frame = pd.DataFrame(rows, columns=CCRE_REF_COLUMNS)
    return frame


def _parquet_available() -> bool:
    try:
        pd.io.parquet.get_engine("auto")
        return True
    except Exception:
        return False


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
    """Parse cCRE BED input and materialize the canonical ccre_ref table."""
    records = parse_ccre_bed(
        bed_path,
        context_group=context_group,
        source_release=source_release,
    )
    written_path = write_ccre_ref(records, output_path, output_format=output_format)
    return IngestResult(row_count=len(records), output_path=written_path, output_format=output_format)
