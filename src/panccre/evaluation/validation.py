"""Validation-link joins, holdout generation, and leakage audits."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Iterator, Mapping

import pandas as pd

from panccre.state_calling import CCRE_STATE_COLUMNS

ASSAY_SOURCE_COLUMNS = [
    "ccre_id",
    "haplotype_id",
    "study_id",
    "assay_type",
    "label",
    "effect_size",
    "cell_context",
    "publication_year",
]

VALIDATION_LINK_COLUMNS = [
    "entity_id",
    "entity_type",
    "study_id",
    "assay_type",
    "label",
    "effect_size",
    "cell_context",
    "publication_year",
    "holdout_group",
]


@dataclass(frozen=True)
class ValidationBuildResult:
    row_count: int
    output_path: Path
    output_format: str


@dataclass(frozen=True)
class HoldoutBuildResult:
    publication_row_count: int
    publication_path: Path
    locus_row_count: int
    locus_path: Path


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
        sep = "\t" if file_path.suffix.lower() in {".tsv", ".tab"} else ","
        frame = pd.read_csv(file_path, sep=sep)
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


def _write_table(frame: pd.DataFrame, path: Path, output_format: str) -> None:
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


def _entity_id(ccre_id: str, haplotype_id: str) -> str:
    return f"{ccre_id}|{haplotype_id}"


def _normalize_assay_row(raw: Mapping[str, object]) -> dict[str, object]:
    missing = [column for column in ASSAY_SOURCE_COLUMNS if column not in raw]
    if missing:
        raise ValueError(f"assay_source row missing required columns: {missing}")
    return {
        "ccre_id": str(raw["ccre_id"]),
        "haplotype_id": str(raw["haplotype_id"]),
        "study_id": str(raw["study_id"]),
        "assay_type": str(raw["assay_type"]),
        "label": str(raw["label"]),
        "effect_size": float(raw["effect_size"]),
        "cell_context": str(raw["cell_context"]),
        "publication_year": int(raw["publication_year"]),
    }


def _iter_state_entity_ids(path: str | Path, *, input_format: str | None = None) -> Iterator[str]:
    file_path = Path(path)
    fmt = (input_format or _infer_format_from_path(file_path)).lower()

    if fmt == "jsonl":
        expected_columns = CCRE_STATE_COLUMNS
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                if not isinstance(payload, dict):
                    raise ValueError("ccre_state JSONL row must decode to object")
                actual = list(payload.keys())
                if actual != expected_columns:
                    raise ValueError(f"column contract mismatch: expected={expected_columns} actual={actual}")
                yield _entity_id(str(payload["ccre_id"]), str(payload["haplotype_id"]))
        return

    if fmt == "csv":
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != CCRE_STATE_COLUMNS:
                raise ValueError(
                    f"column contract mismatch: expected={CCRE_STATE_COLUMNS} actual={reader.fieldnames}"
                )
            for row in reader:
                yield _entity_id(str(row["ccre_id"]), str(row["haplotype_id"]))
        return

    if fmt == "parquet":
        frame = _read_table(file_path, CCRE_STATE_COLUMNS, input_format="parquet")
        for record in frame[["ccre_id", "haplotype_id"]].to_dict(orient="records"):
            yield _entity_id(str(record["ccre_id"]), str(record["haplotype_id"]))
        return

    raise ValueError("input_format must be one of: parquet, csv, jsonl")


def build_validation_link(ccre_state: pd.DataFrame, assay_source: pd.DataFrame) -> pd.DataFrame:
    """Join one assay source against ccre_state entities."""
    if list(ccre_state.columns) != CCRE_STATE_COLUMNS:
        raise ValueError("ccre_state contract mismatch")
    if list(assay_source.columns) != ASSAY_SOURCE_COLUMNS:
        raise ValueError("assay_source contract mismatch")

    state_index = {
        _entity_id(str(row["ccre_id"]), str(row["haplotype_id"])): True
        for _, row in ccre_state.iterrows()
    }

    rows: list[dict[str, object]] = []
    for _, row in assay_source.iterrows():
        entity_id = _entity_id(str(row["ccre_id"]), str(row["haplotype_id"]))
        if entity_id not in state_index:
            continue

        rows.append(
            {
                "entity_id": entity_id,
                "entity_type": "ref_state",
                "study_id": str(row["study_id"]),
                "assay_type": str(row["assay_type"]),
                "label": str(row["label"]),
                "effect_size": float(row["effect_size"]),
                "cell_context": str(row["cell_context"]),
                "publication_year": int(row["publication_year"]),
                "holdout_group": "unassigned",
            }
        )

    link = pd.DataFrame(rows, columns=VALIDATION_LINK_COLUMNS)
    validate_validation_link(link)
    return link


def validate_validation_link(frame: pd.DataFrame) -> None:
    actual = list(frame.columns)
    if actual != VALIDATION_LINK_COLUMNS:
        raise ValueError(f"validation_link column contract mismatch: expected={VALIDATION_LINK_COLUMNS} actual={actual}")
    if frame.empty:
        raise ValueError("validation_link must not be empty")
    valid_labels = {"hit", "non-hit"}
    if not set(frame["label"]).issubset(valid_labels):
        raise ValueError("validation_link label values must be one of: hit, non-hit")


def _assign_publication_holdout(frame: pd.DataFrame) -> pd.DataFrame:
    studies = sorted(frame["study_id"].unique().tolist())
    if len(studies) < 2:
        raise ValueError("publication holdout requires at least two unique study_id values")

    test_study = studies[-1]
    out = frame.copy()
    out["holdout_group"] = out["study_id"].apply(lambda s: "test" if s == test_study else "train")
    return out


def _extract_ccre_id(entity_id: str) -> str:
    return entity_id.split("|", 1)[0]


def _assign_locus_holdout(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()

    def group_for_entity(entity_id: str) -> str:
        ccre_id = _extract_ccre_id(entity_id)
        digits = re.sub(r"\D", "", ccre_id)
        if not digits:
            return "train"
        return "test" if (int(digits) % 5 == 0) else "train"

    out["holdout_group"] = out["entity_id"].apply(group_for_entity)
    return out


def audit_holdout_no_leakage(frame: pd.DataFrame, *, strategy: str) -> None:
    """Raise on leakage according to the selected holdout strategy."""
    if strategy == "publication":
        train_studies = set(frame.loc[frame["holdout_group"] == "train", "study_id"])
        test_studies = set(frame.loc[frame["holdout_group"] == "test", "study_id"])
        overlap = train_studies.intersection(test_studies)
        if overlap:
            raise ValueError(f"Publication leakage detected across studies: {sorted(overlap)}")
        return

    if strategy == "locus":
        train_loci = set(frame.loc[frame["holdout_group"] == "train", "entity_id"].apply(_extract_ccre_id))
        test_loci = set(frame.loc[frame["holdout_group"] == "test", "entity_id"].apply(_extract_ccre_id))
        overlap = train_loci.intersection(test_loci)
        if overlap:
            raise ValueError(f"Locus leakage detected across anchors: {sorted(list(overlap))[:5]}")
        return

    raise ValueError("strategy must be one of: publication, locus")


def build_holdout_views(validation_link: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    publication = _assign_publication_holdout(validation_link)
    locus = _assign_locus_holdout(validation_link)

    audit_holdout_no_leakage(publication, strategy="publication")
    audit_holdout_no_leakage(locus, strategy="locus")

    return publication, locus


def run_validation_link_build(
    *,
    ccre_state_path: str | Path,
    assay_source_path: str | Path,
    output_path: str | Path,
    output_format: str = "jsonl",
    ccre_state_format: str | None = None,
    assay_source_format: str | None = None,
) -> ValidationBuildResult:
    assay_source = _read_table(assay_source_path, ASSAY_SOURCE_COLUMNS, input_format=assay_source_format)
    normalized_assays = [_normalize_assay_row(row.to_dict()) for _, row in assay_source.iterrows()]
    if not normalized_assays:
        raise ValueError(f"Input table is empty: {Path(assay_source_path)}")

    required_entities = {_entity_id(row["ccre_id"], row["haplotype_id"]) for row in normalized_assays}
    available_entities: set[str] = set()
    for entity in _iter_state_entity_ids(ccre_state_path, input_format=ccre_state_format):
        if entity in required_entities:
            available_entities.add(entity)
            if len(available_entities) == len(required_entities):
                break

    rows: list[dict[str, object]] = []
    for row in normalized_assays:
        entity = _entity_id(row["ccre_id"], row["haplotype_id"])
        if entity not in available_entities:
            continue
        rows.append(
            {
                "entity_id": entity,
                "entity_type": "ref_state",
                "study_id": row["study_id"],
                "assay_type": row["assay_type"],
                "label": row["label"],
                "effect_size": row["effect_size"],
                "cell_context": row["cell_context"],
                "publication_year": row["publication_year"],
                "holdout_group": "unassigned",
            }
        )

    link = pd.DataFrame(rows, columns=VALIDATION_LINK_COLUMNS)
    validate_validation_link(link)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    _write_table(link, out, output_format)

    return ValidationBuildResult(row_count=int(link.shape[0]), output_path=out, output_format=output_format)


def run_holdout_build(
    *,
    validation_link_path: str | Path,
    publication_output_path: str | Path,
    locus_output_path: str | Path,
    output_format: str = "jsonl",
    validation_link_format: str | None = None,
) -> HoldoutBuildResult:
    validation_link = _read_table(
        validation_link_path,
        VALIDATION_LINK_COLUMNS,
        input_format=validation_link_format,
    )

    publication, locus = build_holdout_views(validation_link)

    pub_path = Path(publication_output_path)
    loc_path = Path(locus_output_path)
    pub_path.parent.mkdir(parents=True, exist_ok=True)
    loc_path.parent.mkdir(parents=True, exist_ok=True)

    _write_table(publication, pub_path, output_format)
    _write_table(locus, loc_path, output_format)

    return HoldoutBuildResult(
        publication_row_count=int(publication.shape[0]),
        publication_path=pub_path,
        locus_row_count=int(locus.shape[0]),
        locus_path=loc_path,
    )


def write_leakage_summary(
    *,
    publication: pd.DataFrame,
    locus: pd.DataFrame,
    output_path: str | Path,
) -> Path:
    summary = {
        "publication": {
            "row_count": int(publication.shape[0]),
            "train_count": int((publication["holdout_group"] == "train").sum()),
            "test_count": int((publication["holdout_group"] == "test").sum()),
            "study_ids": sorted(publication["study_id"].unique().tolist()),
            "leakage": False,
        },
        "locus": {
            "row_count": int(locus.shape[0]),
            "train_count": int((locus["holdout_group"] == "train").sum()),
            "test_count": int((locus["holdout_group"] == "test").sum()),
            "leakage": False,
        },
    }

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out
