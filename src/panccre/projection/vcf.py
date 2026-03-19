"""VCF-backed haplotype projection adapter."""

from __future__ import annotations

from dataclasses import dataclass
import gzip
from pathlib import Path
import re
from typing import Iterator

import pandas as pd

from panccre.ingest import read_ccre_ref
from panccre.projection.fixture import (
    HAP_PROJECTION_COLUMNS,
    ProjectionResult,
    build_projection_qc_summary,
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


@dataclass
class _VariantAggregate:
    map_status: str = "exact"
    event_count: int = 0
    delta_sum: int = 0
    alt_contig: str | None = None
    has_inversion: bool = False


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


def _collect_variant_aggregates(
    *,
    ccre_ref: pd.DataFrame,
    variants_path: str | Path,
    haplotypes_path: str | Path | None,
    max_variants: int | None = None,
) -> tuple[list[str], dict[tuple[str, str], _VariantAggregate]]:
    variant_file = Path(variants_path)
    if not variant_file.exists():
        raise FileNotFoundError(f"Variant file not found: {variant_file}")
    if max_variants is not None and max_variants <= 0:
        raise ValueError("max_variants must be > 0 when provided")

    ccre_by_chr = {
        str(chrom): frame.reset_index(drop=True)
        for chrom, frame in ccre_ref.groupby("chr", sort=False)
    }

    sample_ids: list[str] = []
    selected_haplotypes: list[str] | None = None
    selected_indexes: list[int] = []
    aggregates: dict[tuple[str, str], _VariantAggregate] = {}

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
        chr_frame = ccre_by_chr.get(chrom)
        if chr_frame is None:
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
        gt_index = 0
        if "GT" in format_keys:
            gt_index = format_keys.index("GT")

        ref_start = pos - 1
        end_from_info = _parse_int(info.get("END"))
        if end_from_info is not None and end_from_info > pos:
            ref_end = end_from_info
        else:
            ref_end = ref_start + max(len(ref_allele), 1)

        overlaps = chr_frame[(chr_frame["start"] < ref_end) & (chr_frame["end"] > ref_start)]
        if overlaps.empty:
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

            haplotype_id = selected_haplotypes[hap_idx]
            for _, ccre_row in overlaps.iterrows():
                key = (str(ccre_row["ccre_id"]), haplotype_id)
                aggregate = aggregates.get(key)
                if aggregate is None:
                    aggregate = _VariantAggregate()
                    aggregates[key] = aggregate

                if _STATUS_PRIORITY[sample_status] > _STATUS_PRIORITY[aggregate.map_status]:
                    aggregate.map_status = sample_status
                aggregate.event_count += 1
                aggregate.delta_sum += sample_delta
                aggregate.alt_contig = chrom
                aggregate.has_inversion = aggregate.has_inversion or sample_has_inversion

        parsed_variants += 1

    if selected_haplotypes is None:
        raise ValueError(f"No #CHROM header found in VCF: {variant_file}")

    if not selected_haplotypes:
        raise ValueError(f"No haplotypes selected from VCF: {variant_file}")

    return (selected_haplotypes, aggregates)


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
    haplotypes, aggregates = _collect_variant_aggregates(
        ccre_ref=ccre_ref,
        variants_path=variants_path,
        haplotypes_path=haplotypes_path,
        max_variants=max_variants,
    )

    rows: list[dict[str, object]] = []
    for _, ccre_row in ccre_ref.reset_index(drop=True).iterrows():
        ref_chr = str(ccre_row["chr"])
        ref_start = int(ccre_row["start"])
        ref_end = int(ccre_row["end"])
        anchor_width = max(ref_end - ref_start, 1)
        for haplotype_id in haplotypes:
            key = (str(ccre_row["ccre_id"]), haplotype_id)
            aggregate = aggregates.get(key)
            if aggregate is None:
                map_status = "exact"
                event_count = 0
                delta_sum = 0
                alt_contig: str | None = ref_chr
                has_inversion = False
            else:
                map_status = aggregate.map_status
                event_count = aggregate.event_count
                delta_sum = aggregate.delta_sum
                alt_contig = aggregate.alt_contig or ref_chr
                has_inversion = aggregate.has_inversion

            coverage_frac, seq_identity, split_count, copy_count, flank_confidence, orientation = _status_metrics(
                map_status, max(event_count, 1)
            )
            if has_inversion and map_status != "absent":
                orientation = "."

            alt_start: int | None
            alt_end: int | None
            if map_status == "absent":
                alt_contig = None
                alt_start = None
                alt_end = None
            else:
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

            rows.append(
                {
                    "ccre_id": str(ccre_row["ccre_id"]),
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
) -> ProjectionResult:
    """Materialize `hap_projection` rows from a VCF/VCF.GZ variant source."""
    frame = build_vcf_hap_projection(
        ccre_ref_path=ccre_ref_path,
        variants_path=variants_path,
        ccre_ref_format=ccre_ref_format,
        haplotypes_path=haplotypes_path,
        max_variants=max_variants,
    )

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    _write_projection_frame(frame, output_file, output_format)

    qc_summary = build_projection_qc_summary(frame)
    qc_file = write_projection_qc_summary(qc_summary, qc_summary_path)

    return ProjectionResult(
        row_count=int(frame.shape[0]),
        output_path=output_file,
        qc_summary_path=qc_file,
        output_format=output_format,
    )
