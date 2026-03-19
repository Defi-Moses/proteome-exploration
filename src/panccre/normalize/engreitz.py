"""Normalize Engreitz CRISPR benchmark rows into the pipeline assay contract."""

from __future__ import annotations

from dataclasses import dataclass
import csv
import gzip
import json
import math
from pathlib import Path
import re
import subprocess
import tempfile
from typing import IO

import pandas as pd

from panccre.evaluation.validation import ASSAY_SOURCE_COLUMNS
from panccre.manifests.builder import compute_sha256
from panccre.projection.fixture import load_haplotype_ids

_REQUIRED_MIN_COLUMNS = {
    "chrom",
    "chromStart",
    "chromEnd",
    "EffectSize",
    "CellType",
}
_YEAR_RE = re.compile(r"(19|20)\d{2}")
_SOURCE_ID_TOKEN_RE = re.compile(r"[^A-Za-z0-9._-]+")
_TRUTHY = {"1", "true", "t", "yes", "y"}
_FALSY = {"0", "false", "f", "no", "n"}


@dataclass(frozen=True)
class NormalizeSummary:
    output_path: Path
    rejects_path: Path
    summary_path: Path
    source_row_count: int
    mapped_row_count: int
    retained_entity_rows: int
    output_row_count: int
    reject_row_count: int
    haplotype_count: int


def _open_text(path: Path) -> IO[str]:
    if path.suffix.lower() == ".gz":
        return gzip.open(path, mode="rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def _normalize_chrom(value: object) -> str:
    text = str(value).strip()
    if not text:
        return text
    if text.startswith("chr"):
        return text
    if text == "MT":
        return "chrM"
    return f"chr{text}"


def _parse_nullable_bool(value: object) -> bool | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text in _TRUTHY:
        return True
    if text in _FALSY:
        return False
    return None


def _parse_publication_year(dataset: object, reference: object) -> int | None:
    for value in (dataset, reference):
        if value is None or pd.isna(value):
            continue
        match = _YEAR_RE.search(str(value))
        if match is not None:
            return int(match.group(0))
    return None


def _normalize_study_id_token(value: str) -> str:
    normalized = _SOURCE_ID_TOKEN_RE.sub("_", value.strip())
    return normalized.strip("_")


def _derive_study_id(dataset: object, reference: object) -> str | None:
    if dataset is not None and not pd.isna(dataset):
        token = _normalize_study_id_token(str(dataset))
        if token:
            return token

    if reference is None or pd.isna(reference):
        return None

    reference_text = str(reference).strip()
    if not reference_text:
        return None

    year = _parse_publication_year("", reference_text)
    author_match = re.search(r"[A-Za-z]+", reference_text)
    if year is None or author_match is None:
        return None
    return f"{author_match.group(0)}_{year}"


def _derive_label(regulated: object, significant: object, effect_size: float | None) -> str | None:
    regulated_flag = _parse_nullable_bool(regulated)
    if regulated_flag is not None:
        return "hit" if regulated_flag else "non-hit"

    significant_flag = _parse_nullable_bool(significant)
    if significant_flag is None or effect_size is None or not math.isfinite(effect_size):
        return None
    if significant_flag and effect_size < 0:
        return "hit"
    return "non-hit"


def _write_source_bed(rows: pd.DataFrame, output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8") as handle:
        for row in rows.itertuples(index=False):
            handle.write(
                f"{row.chrom}\t{int(row.chromStart_int)}\t{int(row.chromEnd_int)}\t{int(row.src_row)}\n"
            )


def _build_ccre_bed4(input_path: Path, output_path: Path) -> None:
    with _open_text(input_path) as source, output_path.open("w", encoding="utf-8") as target:
        for raw in source:
            line = raw.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) < 4:
                fields = line.split()
            if len(fields) < 4:
                continue
            ccre_id = fields[3].strip()
            if not ccre_id:
                continue
            target.write(f"{fields[0]}\t{fields[1]}\t{fields[2]}\t{ccre_id}\n")


def _map_engreitz_rows_to_ccre(
    *,
    source_rows: pd.DataFrame,
    ccre_bed_path: Path,
    bedtools_bin: str,
    min_overlap_bp: int,
) -> pd.DataFrame:
    if source_rows.empty:
        return pd.DataFrame(columns=["src_row", "ccre_id", "overlap_bp", "mid_dist"])

    with tempfile.TemporaryDirectory(prefix="engreitz_map_") as tmpdir:
        tmp = Path(tmpdir)
        source_bed = tmp / "source.bed"
        ccre_bed4 = tmp / "ccre.bed4"
        overlaps = tmp / "overlaps.tsv"

        _write_source_bed(source_rows, source_bed)
        _build_ccre_bed4(ccre_bed_path, ccre_bed4)

        command = [
            bedtools_bin,
            "intersect",
            "-a",
            str(source_bed),
            "-b",
            str(ccre_bed4),
            "-wa",
            "-wb",
        ]
        try:
            with overlaps.open("w", encoding="utf-8") as stdout_handle:
                proc = subprocess.run(
                    command,
                    stdout=stdout_handle,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "bedtools is required for assay normalization but was not found. "
                "Install bedtools and retry."
            ) from exc

        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            raise RuntimeError(f"bedtools intersect failed (exit={proc.returncode}): {stderr}")

        best_by_row: dict[int, tuple[tuple[int, int, str], str, int, int]] = {}
        with overlaps.open("r", encoding="utf-8") as handle:
            reader = csv.reader(handle, delimiter="\t")
            for fields in reader:
                if len(fields) < 8:
                    continue
                src_start = int(fields[1])
                src_end = int(fields[2])
                src_row = int(fields[3])
                ccre_start = int(fields[5])
                ccre_end = int(fields[6])
                ccre_id = str(fields[7]).strip()
                if not ccre_id:
                    continue

                overlap_bp = min(src_end, ccre_end) - max(src_start, ccre_start)
                if overlap_bp <= 0:
                    continue

                src_mid = (src_start + src_end) // 2
                ccre_mid = (ccre_start + ccre_end) // 2
                midpoint_inside = src_start <= ccre_mid < src_end
                if not midpoint_inside and overlap_bp < min_overlap_bp:
                    continue

                mid_dist = abs(src_mid - ccre_mid)
                rank = (-overlap_bp, mid_dist, ccre_id)
                previous = best_by_row.get(src_row)
                if previous is None or rank < previous[0]:
                    best_by_row[src_row] = (rank, ccre_id, overlap_bp, mid_dist)

        rows = [
            {
                "src_row": src_row,
                "ccre_id": ccre_id,
                "overlap_bp": overlap_bp,
                "mid_dist": mid_dist,
            }
            for src_row, (_, ccre_id, overlap_bp, mid_dist) in sorted(best_by_row.items(), key=lambda x: x[0])
        ]
        return pd.DataFrame(rows, columns=["src_row", "ccre_id", "overlap_bp", "mid_dist"])


def _validate_output_contract(frame: pd.DataFrame) -> None:
    actual = list(frame.columns)
    if actual != ASSAY_SOURCE_COLUMNS:
        raise ValueError(
            "assay output columns mismatch: "
            f"expected={ASSAY_SOURCE_COLUMNS} actual={actual}"
        )
    if frame.empty:
        raise ValueError("assay output is empty after normalization")
    if frame.isna().any().any():
        raise ValueError("assay output contains null values")
    labels = set(frame["label"].astype(str).unique().tolist())
    if not labels.issubset({"hit", "non-hit"}):
        raise ValueError(f"assay output contains unsupported labels: {sorted(labels)}")
    if not frame["ccre_id"].astype(str).str.match(r"^EH38E").all():
        raise ValueError("assay output contains ccre_id values outside EH38E namespace")
    if int(frame["study_id"].nunique()) < 2:
        raise ValueError("assay output must contain at least two unique study_id values")


def normalize_engreitz_assay_source(
    *,
    source_path: str | Path,
    ccre_bed_path: str | Path,
    haplotypes_path: str | Path,
    output_path: str | Path,
    rejects_path: str | Path | None = None,
    summary_path: str | Path | None = None,
    bedtools_bin: str = "bedtools",
    min_overlap_bp: int = 50,
    assay_type: str = "CRISPRi",
) -> NormalizeSummary:
    source_file = Path(source_path)
    ccre_file = Path(ccre_bed_path)
    hap_file = Path(haplotypes_path)
    output_file = Path(output_path)
    reject_file = Path(rejects_path) if rejects_path is not None else output_file.with_name(output_file.stem + ".rejects.csv")
    summary_file = Path(summary_path) if summary_path is not None else output_file.with_name(output_file.stem + ".summary.json")

    if not source_file.exists():
        raise FileNotFoundError(f"Assay source not found: {source_file}")
    if not ccre_file.exists():
        raise FileNotFoundError(f"cCRE BED not found: {ccre_file}")
    if not hap_file.exists():
        raise FileNotFoundError(f"Haplotype list not found: {hap_file}")
    if min_overlap_bp <= 0:
        raise ValueError("min_overlap_bp must be > 0")

    source = pd.read_csv(source_file, sep="\t", compression="infer")
    source_row_count = int(source.shape[0])
    if source_row_count == 0:
        raise ValueError(f"Assay source is empty: {source_file}")

    missing_min = sorted(_REQUIRED_MIN_COLUMNS.difference(set(source.columns)))
    if missing_min:
        raise ValueError(f"Assay source missing required columns: {missing_min}")
    if "Regulated" not in source.columns and "Significant" not in source.columns:
        raise ValueError("Assay source requires at least one of Regulated or Significant columns")

    source = source.copy()
    source["src_row"] = source.index.astype(int)
    source["chrom"] = source["chrom"].apply(_normalize_chrom)
    source["chromStart_int"] = pd.to_numeric(source["chromStart"], errors="coerce")
    source["chromEnd_int"] = pd.to_numeric(source["chromEnd"], errors="coerce")
    source["effect_size"] = pd.to_numeric(source["EffectSize"], errors="coerce")

    source["dataset_text"] = source["Dataset"] if "Dataset" in source.columns else ""
    source["reference_text"] = source["Reference"] if "Reference" in source.columns else ""
    source["regulated_value"] = source["Regulated"] if "Regulated" in source.columns else None
    source["significant_value"] = source["Significant"] if "Significant" in source.columns else None

    source["study_id"] = [
        _derive_study_id(dataset, reference)
        for dataset, reference in zip(source["dataset_text"], source["reference_text"])
    ]
    source["publication_year"] = [
        _parse_publication_year(dataset, reference)
        for dataset, reference in zip(source["dataset_text"], source["reference_text"])
    ]
    source["cell_context"] = source["CellType"].fillna("unknown").astype(str).str.strip()
    source.loc[source["cell_context"] == "", "cell_context"] = "unknown"

    source["label"] = [
        _derive_label(regulated, significant, effect if pd.notna(effect) else None)
        for regulated, significant, effect in zip(
            source["regulated_value"],
            source["significant_value"],
            source["effect_size"],
        )
    ]

    valid_interval_mask = (
        source["chrom"].astype(str).str.len() > 0
    ) & source["chromStart_int"].notna() & source["chromEnd_int"].notna() & (
        source["chromEnd_int"] > source["chromStart_int"]
    )

    interval_rows = source.loc[valid_interval_mask, ["src_row", "chrom", "chromStart_int", "chromEnd_int"]].copy()
    mappings = _map_engreitz_rows_to_ccre(
        source_rows=interval_rows,
        ccre_bed_path=ccre_file,
        bedtools_bin=bedtools_bin,
        min_overlap_bp=min_overlap_bp,
    )

    mapped = source.merge(mappings[["src_row", "ccre_id"]], on="src_row", how="left")

    reason_by_row: dict[int, list[str]] = {int(i): [] for i in mapped["src_row"].tolist()}

    for row in mapped.itertuples(index=False):
        reasons = reason_by_row[int(row.src_row)]
        if not (
            isinstance(row.chrom, str)
            and row.chrom
            and pd.notna(row.chromStart_int)
            and pd.notna(row.chromEnd_int)
            and float(row.chromEnd_int) > float(row.chromStart_int)
        ):
            reasons.append("invalid_interval")
        if pd.isna(row.ccre_id) or not str(row.ccre_id).strip():
            reasons.append("no_ccre_match")
        if pd.isna(row.effect_size) or not math.isfinite(float(row.effect_size)):
            reasons.append("invalid_effect_size")
        if row.label is None:
            reasons.append("missing_label")
        if row.study_id is None or not str(row.study_id).strip():
            reasons.append("missing_study_id")
        if row.publication_year is None or pd.isna(row.publication_year):
            reasons.append("missing_publication_year")

    mapped["reject_reason"] = mapped["src_row"].map(
        lambda row_id: ";".join(reason_by_row[int(row_id)])
    )
    rejected = mapped[mapped["reject_reason"].astype(str).str.len() > 0].copy()
    retained = mapped[mapped["reject_reason"].astype(str).str.len() == 0].copy()

    retained_entity_rows = int(retained.shape[0])
    mapped_row_count = int(mappings.shape[0])

    if retained_entity_rows == 0:
        raise ValueError("No rows retained after cCRE mapping and normalization filters")

    haplotypes = load_haplotype_ids(hap_file)
    if not haplotypes:
        raise ValueError(f"No haplotypes found in {hap_file}")

    base = retained[
        [
            "ccre_id",
            "study_id",
            "label",
            "effect_size",
            "cell_context",
            "publication_year",
        ]
    ].copy()
    base["assay_type"] = assay_type

    expanded_parts: list[pd.DataFrame] = []
    for haplotype in haplotypes:
        part = base.copy()
        part.insert(1, "haplotype_id", haplotype)
        expanded_parts.append(part)
    output = pd.concat(expanded_parts, axis=0, ignore_index=True)
    output = output[
        [
            "ccre_id",
            "haplotype_id",
            "study_id",
            "assay_type",
            "label",
            "effect_size",
            "cell_context",
            "publication_year",
        ]
    ]

    # Force contract dtypes for reproducible writes.
    output["ccre_id"] = output["ccre_id"].astype(str)
    output["haplotype_id"] = output["haplotype_id"].astype(str)
    output["study_id"] = output["study_id"].astype(str)
    output["assay_type"] = output["assay_type"].astype(str)
    output["label"] = output["label"].astype(str)
    output["effect_size"] = output["effect_size"].astype(float)
    output["cell_context"] = output["cell_context"].astype(str)
    output["publication_year"] = output["publication_year"].astype(int)

    _validate_output_contract(output)

    output_file.parent.mkdir(parents=True, exist_ok=True)
    reject_file.parent.mkdir(parents=True, exist_ok=True)
    summary_file.parent.mkdir(parents=True, exist_ok=True)

    output.to_csv(output_file, index=False)
    rejected.to_csv(reject_file, index=False)

    summary_payload = {
        "source_path": str(source_file.resolve()),
        "ccre_bed_path": str(ccre_file.resolve()),
        "haplotypes_path": str(hap_file.resolve()),
        "output_path": str(output_file.resolve()),
        "rejects_path": str(reject_file.resolve()),
        "assay_type": assay_type,
        "min_overlap_bp": int(min_overlap_bp),
        "source_row_count": source_row_count,
        "mapped_row_count": mapped_row_count,
        "retained_entity_rows": retained_entity_rows,
        "haplotype_count": len(haplotypes),
        "output_row_count": int(output.shape[0]),
        "reject_row_count": int(rejected.shape[0]),
        "mapping_rate": float(mapped_row_count / source_row_count) if source_row_count else 0.0,
        "retention_rate": float(retained_entity_rows / source_row_count) if source_row_count else 0.0,
        "study_count": int(output["study_id"].nunique()),
        "label_counts": {
            label: int(count) for label, count in output["label"].value_counts().sort_index().items()
        },
        "checksums": {
            "output_sha256": compute_sha256(output_file),
            "rejects_sha256": compute_sha256(reject_file),
        },
    }
    summary_file.write_text(json.dumps(summary_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return NormalizeSummary(
        output_path=output_file,
        rejects_path=reject_file,
        summary_path=summary_file,
        source_row_count=source_row_count,
        mapped_row_count=mapped_row_count,
        retained_entity_rows=retained_entity_rows,
        output_row_count=int(output.shape[0]),
        reject_row_count=int(rejected.shape[0]),
        haplotype_count=len(haplotypes),
    )


__all__ = [
    "NormalizeSummary",
    "normalize_engreitz_assay_source",
]
