"""VCF-backed haplotype projection adapter."""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
import gzip
import json
from pathlib import Path
import re
from typing import Iterator, TextIO

import pandas as pd

from panccre.ingest import read_ccre_ref
from panccre.projection.fixture import (
    HAP_PROJECTION_COLUMNS,
    ProjectionResult,
    load_haplotype_ids,
    validate_hap_projection_frame,
    write_projection_qc_summary,
)

_STATUS_PRIORITY = {
    "exact": 0,
    "diverged": 1,
    "fractured": 2,
    "duplicated": 3,
    "absent": 4,
    "ambiguous": 1,
}

_GENOTYPE_SPLIT_RE = re.compile(r"[\/|]")
_DEFAULT_STREAM_CHUNK_ROWS = 20_000


@dataclass
class _VariantAggregate:
    map_status: str = "exact"
    event_count: int = 0
    delta_sum: int = 0
    has_inversion: bool = False


@dataclass
class _ChromSweepState:
    next_start_index: int = 0
    active: list[tuple[int, int]] = field(default_factory=list)
    last_ref_start: int | None = None


@dataclass(frozen=True)
class _CCREIndex:
    ccre_ids: list[str]
    chroms: list[str]
    starts: list[int]
    ends: list[int]
    chrom_to_sorted_anchor_indices: dict[str, list[int]]
    chrom_to_sorted_starts: dict[str, list[int]]


@dataclass
class _ProjectionSummaryAccumulator:
    row_count: int = 0
    map_status_counts: dict[str, int] = field(default_factory=dict)
    coverage_sum: float = 0.0
    coverage_min: float | None = None
    coverage_max: float | None = None
    seq_identity_sum: float = 0.0
    seq_identity_min: float | None = None
    seq_identity_max: float | None = None

    def update(self, *, map_status: str, coverage_frac: float, seq_identity: float) -> None:
        self.row_count += 1
        self.map_status_counts[map_status] = self.map_status_counts.get(map_status, 0) + 1

        self.coverage_sum += coverage_frac
        if self.coverage_min is None or coverage_frac < self.coverage_min:
            self.coverage_min = coverage_frac
        if self.coverage_max is None or coverage_frac > self.coverage_max:
            self.coverage_max = coverage_frac

        self.seq_identity_sum += seq_identity
        if self.seq_identity_min is None or seq_identity < self.seq_identity_min:
            self.seq_identity_min = seq_identity
        if self.seq_identity_max is None or seq_identity > self.seq_identity_max:
            self.seq_identity_max = seq_identity

    def summary(self, *, unique_ccre_ids: int, unique_haplotype_ids: int) -> dict[str, object]:
        if self.row_count <= 0:
            raise ValueError("hap_projection row_count must be > 0")

        return {
            "row_count": int(self.row_count),
            "unique_ccre_ids": int(unique_ccre_ids),
            "unique_haplotype_ids": int(unique_haplotype_ids),
            "map_status_counts": {
                status: int(self.map_status_counts[status])
                for status in sorted(self.map_status_counts)
            },
            "coverage_frac": {
                "min": float(self.coverage_min if self.coverage_min is not None else 0.0),
                "mean": float(self.coverage_sum / self.row_count),
                "max": float(self.coverage_max if self.coverage_max is not None else 0.0),
            },
            "seq_identity": {
                "min": float(self.seq_identity_min if self.seq_identity_min is not None else 0.0),
                "mean": float(self.seq_identity_sum / self.row_count),
                "max": float(self.seq_identity_max if self.seq_identity_max is not None else 0.0),
            },
        }


class _ProjectionRowWriter:
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
                fieldnames=HAP_PROJECTION_COLUMNS,
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


def _write_projection_frame(frame: pd.DataFrame, path: Path, output_format: str) -> None:
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


def _open_variant_text(path: Path) -> Iterator[str]:
    if path.suffix.lower() == ".gz":
        with gzip.open(path, mode="rt", encoding="utf-8") as handle:
            for line in handle:
                yield line
        return

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            yield line


def _normalize_chrom(chrom: str) -> str:
    value = chrom.strip()
    if not value:
        return value
    if value.startswith("chr"):
        return value
    if value == "MT":
        return "chrM"
    return f"chr{value}"


def _parse_info_field(info_field: str) -> dict[str, str]:
    if info_field == ".":
        return {}

    info: dict[str, str] = {}
    for token in info_field.split(";"):
        token = token.strip()
        if not token:
            continue
        if "=" not in token:
            info[token] = "1"
            continue
        key, value = token.split("=", 1)
        info[key] = value
    return info


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    token = text.split(",")[0].strip()
    try:
        return int(token)
    except ValueError:
        return None


def _symbolic_alt_type(alt_allele: str) -> str | None:
    text = alt_allele.strip()
    if not (text.startswith("<") and text.endswith(">")):
        return None
    return text[1:-1].upper()


def _allele_length(ref_allele: str, alt_allele: str, info: dict[str, str]) -> int:
    symbolic = _symbolic_alt_type(alt_allele)
    ref_len = max(len(ref_allele), 1)
    if symbolic is None:
        return max(len(alt_allele), 1)

    svlen = _parse_int(info.get("SVLEN"))
    if svlen is not None:
        if svlen >= 0:
            return max(svlen, 1)
        return max(ref_len + svlen, 1)

    if symbolic == "DEL":
        return 1
    if symbolic in {"DUP", "CNV", "INS"}:
        return max(ref_len * 2, 1)
    return ref_len


def _classify_variant(ref_allele: str, alt_allele: str, info: dict[str, str]) -> tuple[str, int, bool]:
    svtype = info.get("SVTYPE", "").upper()
    symbolic = _symbolic_alt_type(alt_allele)
    if symbolic and not svtype:
        svtype = symbolic

    ref_len = max(len(ref_allele), 1)
    alt_len = _allele_length(ref_allele, alt_allele, info)
    delta = int(alt_len - ref_len)

    if svtype in {"DEL"}:
        return ("absent", delta if delta < 0 else -max(ref_len, 1), False)
    if svtype in {"DUP", "CNV"}:
        return ("duplicated", delta if delta > 0 else ref_len, False)
    if svtype in {"INV", "BND", "TRA"}:
        return ("fractured", delta, True)
    if svtype in {"INS"}:
        if delta >= 100:
            return ("duplicated", delta, False)
        return ("diverged", delta, False)

    if delta <= -100:
        return ("absent", delta, False)
    if delta < 0:
        return ("fractured", delta, False)
    if delta >= 100:
        return ("duplicated", delta, False)
    return ("diverged", delta, False)


def _status_metrics(map_status: str, event_count: int) -> tuple[float, float, int, int, float, str]:
    if map_status == "exact":
        return (0.99, 0.998, 1, 1, 0.98, "+")
    if map_status == "diverged":
        coverage = max(0.55, 0.84 - (event_count - 1) * 0.03)
        identity = max(0.85, 0.955 - (event_count - 1) * 0.01)
        flank = max(0.60, 0.92 - (event_count - 1) * 0.03)
        return (coverage, identity, 1, 1, flank, "+")
    if map_status == "fractured":
        coverage = max(0.25, 0.61 - (event_count - 1) * 0.04)
        identity = max(0.75, 0.931 - (event_count - 1) * 0.015)
        return (coverage, identity, max(2, event_count), 1, 0.77, "+")
    if map_status == "absent":
        return (0.0, 0.0, max(1, event_count), 0, 0.35, ".")
    if map_status == "duplicated":
        copy_count = max(2, event_count + 1)
        return (0.98, 0.997, 1, copy_count, 0.96, "+")
    return (0.72, 0.941, 1, 1, 0.58, ".")


def _parse_non_reference_alleles(genotype_token: str) -> list[int]:
    token = genotype_token.strip()
    if token in {"", ".", "./.", ".|."}:
        return []
    if token == "0":
        return []

    values: list[int] = []
    for part in _GENOTYPE_SPLIT_RE.split(token):
        if part in {"", "."}:
            continue
        try:
            allele_index = int(part)
        except ValueError:
            continue
        if allele_index > 0:
            values.append(allele_index)
    return values


def _select_haplotypes(sample_ids: list[str], haplotypes_path: str | Path | None) -> tuple[list[str], list[int]]:
    if haplotypes_path is None:
        return (sample_ids, list(range(len(sample_ids))))

    requested = load_haplotype_ids(haplotypes_path)
    sample_index = {sample: idx for idx, sample in enumerate(sample_ids)}

    missing = [sample for sample in requested if sample not in sample_index]
    if missing:
        raise ValueError(
            "Requested haplotype IDs not found in VCF samples: " + ", ".join(missing)
        )

    indexes = [sample_index[sample] for sample in requested]
    return (requested, indexes)


def _build_ccre_index(ccre_ref: pd.DataFrame) -> _CCREIndex:
    frame = ccre_ref.reset_index(drop=True)
    ccre_ids = frame["ccre_id"].astype(str).tolist()
    chroms = frame["chr"].astype(str).tolist()
    starts = frame["start"].astype(int).tolist()
    ends = frame["end"].astype(int).tolist()

    chrom_to_anchor_indices: dict[str, list[int]] = {}
    for anchor_idx, chrom in enumerate(chroms):
        chrom_to_anchor_indices.setdefault(chrom, []).append(anchor_idx)

    chrom_to_sorted_anchor_indices: dict[str, list[int]] = {}
    chrom_to_sorted_starts: dict[str, list[int]] = {}
    for chrom, anchor_indices in chrom_to_anchor_indices.items():
        sorted_indices = sorted(anchor_indices, key=lambda idx: starts[idx])
        chrom_to_sorted_anchor_indices[chrom] = sorted_indices
        chrom_to_sorted_starts[chrom] = [starts[idx] for idx in sorted_indices]

    return _CCREIndex(
        ccre_ids=ccre_ids,
        chroms=chroms,
        starts=starts,
        ends=ends,
        chrom_to_sorted_anchor_indices=chrom_to_sorted_anchor_indices,
        chrom_to_sorted_starts=chrom_to_sorted_starts,
    )


def _overlapping_anchor_indices(
    *,
    ccre_index: _CCREIndex,
    chrom: str,
    ref_start: int,
    ref_end: int,
    sweep_states: dict[str, _ChromSweepState],
) -> list[int]:
    starts = ccre_index.chrom_to_sorted_starts.get(chrom)
    if starts is None:
        return []

    sorted_anchor_indices = ccre_index.chrom_to_sorted_anchor_indices[chrom]
    state = sweep_states.get(chrom)
    if state is None:
        state = _ChromSweepState()
        sweep_states[chrom] = state

    if state.last_ref_start is not None and ref_start < state.last_ref_start:
        # Fallback for non-monotonic variant coordinates within chromosome.
        state.next_start_index = 0
        state.active.clear()
    state.last_ref_start = ref_start

    next_start_index = state.next_start_index
    starts_len = len(starts)
    while next_start_index < starts_len and starts[next_start_index] < ref_end:
        anchor_idx = sorted_anchor_indices[next_start_index]
        state.active.append((ccre_index.ends[anchor_idx], anchor_idx))
        next_start_index += 1
    state.next_start_index = next_start_index

    if not state.active:
        return []

    next_active: list[tuple[int, int]] = []
    overlaps: list[int] = []
    for anchor_end, anchor_idx in state.active:
        if anchor_end > ref_start:
            next_active.append((anchor_end, anchor_idx))
            overlaps.append(anchor_idx)

    state.active = next_active
    return overlaps


def _collect_variant_aggregates(
    *,
    ccre_index: _CCREIndex,
    variants_path: str | Path,
    haplotypes_path: str | Path | None,
    max_variants: int | None = None,
) -> tuple[list[str], dict[int, _VariantAggregate]]:
    variant_file = Path(variants_path)
    if not variant_file.exists():
        raise FileNotFoundError(f"Variant file not found: {variant_file}")
    if max_variants is not None and max_variants <= 0:
        raise ValueError("max_variants must be > 0 when provided")

    sample_ids: list[str] = []
    selected_haplotypes: list[str] | None = None
    selected_indexes: list[int] = []
    haplotype_count = 0

    aggregates: dict[int, _VariantAggregate] = {}
    sweep_states: dict[str, _ChromSweepState] = {}

    parsed_variants = 0
    for raw_line in _open_variant_text(variant_file):
        if not raw_line.strip():
            continue
        if raw_line.startswith("##"):
            continue
        if raw_line.startswith("#CHROM"):
            fields = raw_line.rstrip("\n").split("\t")
            if len(fields) < 10:
                raise ValueError(f"VCF header has no sample columns: {variant_file}")
            sample_ids = fields[9:]
            selected_haplotypes, selected_indexes = _select_haplotypes(sample_ids, haplotypes_path)
            haplotype_count = len(selected_haplotypes)
            continue
        if raw_line.startswith("#"):
            continue

        if selected_haplotypes is None:
            raise ValueError(f"VCF missing #CHROM header before records: {variant_file}")

        if max_variants is not None and parsed_variants >= max_variants:
            break

        fields = raw_line.rstrip("\n").split("\t")
        if len(fields) < 10:
            continue

        chrom = _normalize_chrom(fields[0])
        if chrom not in ccre_index.chrom_to_sorted_starts:
            parsed_variants += 1
            continue

        try:
            pos = int(fields[1])
        except ValueError:
            parsed_variants += 1
            continue

        ref_allele = fields[3]
        alt_field = fields[4]
        if alt_field == ".":
            parsed_variants += 1
            continue

        alt_alleles = [token.strip() for token in alt_field.split(",") if token.strip()]
        if not alt_alleles:
            parsed_variants += 1
            continue

        info = _parse_info_field(fields[7])
        format_keys = fields[8].split(":")
        gt_index = format_keys.index("GT") if "GT" in format_keys else 0

        ref_start = pos - 1
        end_from_info = _parse_int(info.get("END"))
        if end_from_info is not None and end_from_info > pos:
            ref_end = end_from_info
        else:
            ref_end = ref_start + max(len(ref_allele), 1)

        overlapping_anchor_indices = _overlapping_anchor_indices(
            ccre_index=ccre_index,
            chrom=chrom,
            ref_start=ref_start,
            ref_end=ref_end,
            sweep_states=sweep_states,
        )
        if not overlapping_anchor_indices:
            parsed_variants += 1
            continue

        sample_columns = fields[9:]
        for hap_idx, sample_offset in enumerate(selected_indexes):
            if sample_offset >= len(sample_columns):
                continue

            sample_token = sample_columns[sample_offset]
            sample_fields = sample_token.split(":")
            genotype = sample_fields[gt_index] if gt_index < len(sample_fields) else sample_fields[0]

            non_ref_alleles = _parse_non_reference_alleles(genotype)
            if not non_ref_alleles:
                continue

            sample_status = "exact"
            sample_delta = 0
            sample_has_inversion = False
            for allele_index in non_ref_alleles:
                if allele_index <= 0 or allele_index > len(alt_alleles):
                    continue
                status, delta, has_inversion = _classify_variant(ref_allele, alt_alleles[allele_index - 1], info)
                if _STATUS_PRIORITY[status] > _STATUS_PRIORITY[sample_status]:
                    sample_status = status
                sample_delta += delta
                sample_has_inversion = sample_has_inversion or has_inversion

            if sample_status == "exact":
                continue

            for anchor_idx in overlapping_anchor_indices:
                packed_key = anchor_idx * haplotype_count + hap_idx
                aggregate = aggregates.get(packed_key)
                if aggregate is None:
                    aggregate = _VariantAggregate()
                    aggregates[packed_key] = aggregate

                if _STATUS_PRIORITY[sample_status] > _STATUS_PRIORITY[aggregate.map_status]:
                    aggregate.map_status = sample_status
                aggregate.event_count += 1
                aggregate.delta_sum += sample_delta
                aggregate.has_inversion = aggregate.has_inversion or sample_has_inversion

        parsed_variants += 1

    if selected_haplotypes is None:
        raise ValueError(f"No #CHROM header found in VCF: {variant_file}")

    if not selected_haplotypes:
        raise ValueError(f"No haplotypes selected from VCF: {variant_file}")

    return (selected_haplotypes, aggregates)


def _iter_projection_rows(
    *,
    ccre_index: _CCREIndex,
    haplotypes: list[str],
    aggregates: dict[int, _VariantAggregate],
) -> Iterator[dict[str, object]]:
    haplotype_count = len(haplotypes)

    for anchor_idx, ccre_id in enumerate(ccre_index.ccre_ids):
        ref_chr = ccre_index.chroms[anchor_idx]
        ref_start = ccre_index.starts[anchor_idx]
        ref_end = ccre_index.ends[anchor_idx]
        anchor_width = max(ref_end - ref_start, 1)
        key_base = anchor_idx * haplotype_count

        for hap_idx, haplotype_id in enumerate(haplotypes):
            aggregate = aggregates.get(key_base + hap_idx)
            if aggregate is None:
                map_status = "exact"
                event_count = 0
                delta_sum = 0
                has_inversion = False
            else:
                map_status = aggregate.map_status
                event_count = aggregate.event_count
                delta_sum = aggregate.delta_sum
                has_inversion = aggregate.has_inversion

            coverage_frac, seq_identity, split_count, copy_count, flank_confidence, orientation = _status_metrics(
                map_status,
                max(event_count, 1),
            )
            if has_inversion and map_status != "absent":
                orientation = "."

            alt_contig: str | None
            alt_start: int | None
            alt_end: int | None
            if map_status == "absent":
                alt_contig = None
                alt_start = None
                alt_end = None
            else:
                alt_contig = ref_chr
                shift = max(-10, min(10, delta_sum))
                alt_start = max(0, ref_start + shift)

                if map_status == "exact":
                    alt_len = anchor_width
                elif map_status == "diverged":
                    bounded_delta = max(-20, min(20, delta_sum))
                    alt_len = max(1, anchor_width + bounded_delta)
                elif map_status == "fractured":
                    bounded_delta = min(delta_sum, -1)
                    alt_len = max(1, min(anchor_width - 1 if anchor_width > 1 else 1, anchor_width + bounded_delta))
                elif map_status == "duplicated":
                    bounded_delta = max(delta_sum, 1)
                    alt_len = max(anchor_width + 1, anchor_width + bounded_delta)
                else:
                    alt_len = anchor_width
                alt_end = alt_start + alt_len

            yield {
                "ccre_id": ccre_id,
                "haplotype_id": haplotype_id,
                "ref_chr": ref_chr,
                "ref_start": ref_start,
                "ref_end": ref_end,
                "alt_contig": alt_contig,
                "alt_start": alt_start,
                "alt_end": alt_end,
                "orientation": orientation,
                "map_status": map_status,
                "coverage_frac": float(coverage_frac),
                "seq_identity": float(seq_identity),
                "split_count": int(split_count),
                "copy_count": int(copy_count),
                "flank_synteny_confidence": float(flank_confidence),
                "mapping_method": "vcf_projection_v1",
            }


def build_vcf_hap_projection(
    *,
    ccre_ref_path: str | Path,
    variants_path: str | Path,
    ccre_ref_format: str | None = None,
    haplotypes_path: str | Path | None = None,
    max_variants: int | None = None,
) -> pd.DataFrame:
    """Build `hap_projection` rows by intersecting cCRE anchors with VCF variants."""
    ccre_ref = read_ccre_ref(ccre_ref_path, input_format=ccre_ref_format)
    ccre_index = _build_ccre_index(ccre_ref)

    haplotypes, aggregates = _collect_variant_aggregates(
        ccre_index=ccre_index,
        variants_path=variants_path,
        haplotypes_path=haplotypes_path,
        max_variants=max_variants,
    )

    rows = list(
        _iter_projection_rows(
            ccre_index=ccre_index,
            haplotypes=haplotypes,
            aggregates=aggregates,
        )
    )
    frame = pd.DataFrame(rows, columns=HAP_PROJECTION_COLUMNS)
    validate_hap_projection_frame(frame)
    return frame


def project_vcf_haplotypes(
    *,
    ccre_ref_path: str | Path,
    variants_path: str | Path,
    output_path: str | Path,
    qc_summary_path: str | Path,
    output_format: str = "jsonl",
    ccre_ref_format: str | None = None,
    haplotypes_path: str | Path | None = None,
    max_variants: int | None = None,
    stream_chunk_rows: int = _DEFAULT_STREAM_CHUNK_ROWS,
) -> ProjectionResult:
    """Materialize `hap_projection` rows from a VCF/VCF.GZ variant source."""
    ccre_ref = read_ccre_ref(ccre_ref_path, input_format=ccre_ref_format)
    ccre_index = _build_ccre_index(ccre_ref)
    del ccre_ref

    haplotypes, aggregates = _collect_variant_aggregates(
        ccre_index=ccre_index,
        variants_path=variants_path,
        haplotypes_path=haplotypes_path,
        max_variants=max_variants,
    )

    output_file = Path(output_path)
    writer = _ProjectionRowWriter(path=output_file, output_format=output_format, chunk_rows=stream_chunk_rows)
    summary = _ProjectionSummaryAccumulator()

    try:
        for row in _iter_projection_rows(
            ccre_index=ccre_index,
            haplotypes=haplotypes,
            aggregates=aggregates,
        ):
            writer.write_row(row)
            summary.update(
                map_status=str(row["map_status"]),
                coverage_frac=float(row["coverage_frac"]),
                seq_identity=float(row["seq_identity"]),
            )
    finally:
        writer.close()

    qc_summary = summary.summary(
        unique_ccre_ids=len(ccre_index.ccre_ids),
        unique_haplotype_ids=len(haplotypes),
    )
    qc_file = write_projection_qc_summary(qc_summary, qc_summary_path)

    return ProjectionResult(
        row_count=int(summary.row_count),
        output_path=output_file,
        qc_summary_path=qc_file,
        output_format=output_format,
    )
