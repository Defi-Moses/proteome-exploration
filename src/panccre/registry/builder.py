"""Registry artifact builder for phase-1 outputs."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

import pandas as pd

from panccre.candidate_discovery import REPLACEMENT_CANDIDATE_COLUMNS
from panccre.evaluation import VALIDATION_LINK_COLUMNS
from panccre.scorers import SCORER_OUTPUT_COLUMNS
from panccre.state_calling import CCRE_STATE_COLUMNS

REGISTRY_COLUMNS = [
    "entity_id",
    "source_anchor_ccre",
    "haplotype_id",
    "state_class",
    "ref_chr",
    "ref_start",
    "ref_end",
    "alt_contig",
    "alt_start",
    "alt_end",
    "context_group",
    "provenance",
    "evidence_summary",
    "ranking_score",
    "qc_flag",
]


@dataclass(frozen=True)
class RegistryBuildResult:
    output_dir: Path
    output_format: str
    registry_rows: int


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


def build_polymorphic_registry(
    ccre_state: pd.DataFrame,
    scorer_output: pd.DataFrame,
    validation_link: pd.DataFrame,
    *,
    context_group: str,
) -> pd.DataFrame:
    """Build polymorphic registry rows with evidence summary."""
    if list(ccre_state.columns) != CCRE_STATE_COLUMNS:
        raise ValueError("ccre_state contract mismatch")
    if list(scorer_output.columns) != SCORER_OUTPUT_COLUMNS:
        raise ValueError("scorer_output contract mismatch")
    if list(validation_link.columns) != VALIDATION_LINK_COLUMNS:
        raise ValueError("validation_link contract mismatch")

    score_by_entity = (
        scorer_output.groupby(["entity_id"], as_index=False)["delta_score"].mean().rename(columns={"delta_score": "ranking_score"})
    )
    score_map = dict(zip(score_by_entity["entity_id"], score_by_entity["ranking_score"]))

    assays_by_entity: dict[str, dict[str, object]] = {}
    for entity_id, group in validation_link.groupby("entity_id"):
        hit_count = int((group["label"] == "hit").sum())
        assays_by_entity[str(entity_id)] = {
            "hit_count": hit_count,
            "total_labels": int(group.shape[0]),
            "assay_types": sorted(group["assay_type"].astype(str).unique().tolist()),
            "study_ids": sorted(group["study_id"].astype(str).unique().tolist()),
        }

    rows: list[dict[str, object]] = []
    for _, row in ccre_state.iterrows():
        ccre_id = str(row["ccre_id"])
        haplotype_id = str(row["haplotype_id"])
        entity_id = _entity_id(ccre_id, haplotype_id)

        reason = json.loads(str(row["state_reason"]))
        evidence = assays_by_entity.get(entity_id, {"hit_count": 0, "total_labels": 0, "assay_types": [], "study_ids": []})

        rows.append(
            {
                "entity_id": entity_id,
                "source_anchor_ccre": ccre_id,
                "haplotype_id": haplotype_id,
                "state_class": str(row["state_class"]),
                "ref_chr": str(reason.get("ref_chr", "")),
                "ref_start": int(reason.get("ref_start", 0)),
                "ref_end": int(reason.get("ref_end", 0)),
                "alt_contig": None if reason.get("alt_contig") is None else str(reason.get("alt_contig")),
                "alt_start": None if reason.get("alt_start") is None else int(reason.get("alt_start")),
                "alt_end": None if reason.get("alt_end") is None else int(reason.get("alt_end")),
                "context_group": context_group,
                "provenance": json.dumps(
                    {
                        "state_source": "ccre_state",
                        "scorer_source": "scorer_output",
                        "validation_source": "validation_link",
                    },
                    sort_keys=True,
                ),
                "evidence_summary": json.dumps(evidence, sort_keys=True),
                "ranking_score": float(score_map.get(entity_id, 0.0)),
                "qc_flag": str(row["qc_flag"]),
            }
        )

    registry = pd.DataFrame(rows, columns=REGISTRY_COLUMNS)
    validate_polymorphic_registry(registry)
    return registry


def validate_polymorphic_registry(frame: pd.DataFrame) -> None:
    if list(frame.columns) != REGISTRY_COLUMNS:
        raise ValueError(f"registry column contract mismatch: expected={REGISTRY_COLUMNS} actual={list(frame.columns)}")
    if frame.empty:
        raise ValueError("registry must not be empty")
    if frame["entity_id"].duplicated().any():
        raise ValueError("registry contains duplicate entity_id values")


def run_registry_build(
    *,
    ccre_state_path: str | Path,
    replacement_candidates_path: str | Path,
    scorer_output_path: str | Path,
    validation_link_path: str | Path,
    output_dir: str | Path,
    output_format: str = "jsonl",
    ccre_state_format: str | None = None,
    replacement_candidates_format: str | None = None,
    scorer_output_format: str | None = None,
    validation_link_format: str | None = None,
    context_group: str = "immune_hematopoietic",
) -> RegistryBuildResult:
    ccre_state = _read_table(ccre_state_path, CCRE_STATE_COLUMNS, input_format=ccre_state_format)
    replacement = _read_table(
        replacement_candidates_path,
        REPLACEMENT_CANDIDATE_COLUMNS,
        input_format=replacement_candidates_format,
    )
    scorer_output = _read_table(scorer_output_path, SCORER_OUTPUT_COLUMNS, input_format=scorer_output_format)
    validation_link = _read_table(validation_link_path, VALIDATION_LINK_COLUMNS, input_format=validation_link_format)

    registry = build_polymorphic_registry(
        ccre_state,
        scorer_output,
        validation_link,
        context_group=context_group,
    )

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    extension = "parquet" if output_format == "parquet" else ("csv" if output_format == "csv" else "jsonl")
    registry_path = out_dir / f"polymorphic_ccre_registry.{extension}"
    replacement_path = out_dir / f"replacement_candidates.{extension}"
    scorer_path = out_dir / f"scorer_outputs.{extension}"
    validation_path = out_dir / f"validation_links.{extension}"

    _write_table(registry, registry_path, output_format)
    _write_table(replacement, replacement_path, output_format)
    _write_table(scorer_output, scorer_path, output_format)
    _write_table(validation_link, validation_path, output_format)

    manifest = {
        "output_format": output_format,
        "files": {
            "polymorphic_ccre_registry": str(registry_path.resolve()),
            "replacement_candidates": str(replacement_path.resolve()),
            "scorer_outputs": str(scorer_path.resolve()),
            "validation_links": str(validation_path.resolve()),
        },
        "row_counts": {
            "polymorphic_ccre_registry": int(registry.shape[0]),
            "replacement_candidates": int(replacement.shape[0]),
            "scorer_outputs": int(scorer_output.shape[0]),
            "validation_links": int(validation_link.shape[0]),
        },
    }
    (out_dir / "registry_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return RegistryBuildResult(output_dir=out_dir, output_format=output_format, registry_rows=int(registry.shape[0]))
