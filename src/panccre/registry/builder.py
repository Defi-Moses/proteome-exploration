"""Registry artifact builder for phase-1 outputs."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterator, Mapping, TextIO

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


_DEFAULT_STREAM_CHUNK_ROWS = 20_000


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
                    raise ValueError(f"column contract mismatch: expected={expected_columns} actual={actual}")
                yield payload
        return

    if fmt == "csv":
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != expected_columns:
                raise ValueError(f"column contract mismatch: expected={expected_columns} actual={reader.fieldnames}")
            for row in reader:
                yield row
        return

    if fmt == "parquet":
        frame = _read_table(file_path, expected_columns, input_format="parquet")
        for record in frame.to_dict(orient="records"):
            yield record
        return

    raise ValueError("input_format must be one of: parquet, csv, jsonl")


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


class _TableRowWriter:
    def __init__(self, *, path: str | Path, columns: list[str], output_format: str, chunk_rows: int) -> None:
        self.path = Path(path)
        self.columns = list(columns)
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
                fieldnames=self.columns,
                extrasaction="raise",
            )
            self._csv_writer.writeheader()
        elif self.output_format == "parquet":
            if not _parquet_available():
                raise RuntimeError(
                    "Parquet output requires pyarrow or fastparquet. "
                    "Install one of those engines or choose --output-format csv/jsonl."
                )

    def write_row(self, row: Mapping[str, object]) -> None:
        self._buffer.append({column: row.get(column) for column in self.columns})
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


def _entity_id(ccre_id: str, haplotype_id: str) -> str:
    return f"{ccre_id}|{haplotype_id}"


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _safe_reason_map(raw: object) -> dict[str, object]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


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
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    extension = "parquet" if output_format == "parquet" else ("csv" if output_format == "csv" else "jsonl")
    registry_path = out_dir / f"polymorphic_ccre_registry.{extension}"
    replacement_path = out_dir / f"replacement_candidates.{extension}"
    scorer_path = out_dir / f"scorer_outputs.{extension}"
    validation_path = out_dir / f"validation_links.{extension}"

    validation_writer = _TableRowWriter(
        path=validation_path,
        columns=VALIDATION_LINK_COLUMNS,
        output_format=output_format,
        chunk_rows=_DEFAULT_STREAM_CHUNK_ROWS,
    )
    assays_by_entity: dict[str, dict[str, object]] = {}
    validation_rows = 0
    try:
        for raw in _iter_table_rows(
            validation_link_path,
            VALIDATION_LINK_COLUMNS,
            input_format=validation_link_format,
        ):
            row = {column: raw.get(column) for column in VALIDATION_LINK_COLUMNS}
            validation_writer.write_row(row)
            validation_rows += 1

            entity_id = str(row["entity_id"])
            entry = assays_by_entity.setdefault(
                entity_id,
                {"hit_count": 0, "total_labels": 0, "assay_types": set(), "study_ids": set()},
            )
            if str(row["label"]) == "hit":
                entry["hit_count"] = int(entry["hit_count"]) + 1
            entry["total_labels"] = int(entry["total_labels"]) + 1
            cast_assay_types = entry["assay_types"]
            cast_study_ids = entry["study_ids"]
            assert isinstance(cast_assay_types, set) and isinstance(cast_study_ids, set)
            cast_assay_types.add(str(row["assay_type"]))
            cast_study_ids.add(str(row["study_id"]))
    finally:
        validation_writer.close()
    if validation_rows <= 0:
        raise ValueError(f"Input table is empty: {Path(validation_link_path)}")

    replacement_writer = _TableRowWriter(
        path=replacement_path,
        columns=REPLACEMENT_CANDIDATE_COLUMNS,
        output_format=output_format,
        chunk_rows=_DEFAULT_STREAM_CHUNK_ROWS,
    )
    replacement_rows = 0
    try:
        for raw in _iter_table_rows(
            replacement_candidates_path,
            REPLACEMENT_CANDIDATE_COLUMNS,
            input_format=replacement_candidates_format,
        ):
            replacement_writer.write_row(raw)
            replacement_rows += 1
    finally:
        replacement_writer.close()
    if replacement_rows <= 0:
        raise ValueError(f"Input table is empty: {Path(replacement_candidates_path)}")

    scorer_writer = _TableRowWriter(
        path=scorer_path,
        columns=SCORER_OUTPUT_COLUMNS,
        output_format=output_format,
        chunk_rows=_DEFAULT_STREAM_CHUNK_ROWS,
    )
    score_agg_by_entity: dict[str, list[float]] = {}
    scorer_rows = 0
    try:
        for raw in _iter_table_rows(
            scorer_output_path,
            SCORER_OUTPUT_COLUMNS,
            input_format=scorer_output_format,
        ):
            scorer_writer.write_row(raw)
            scorer_rows += 1
            if str(raw["entity_type"]) != "ref_state":
                continue
            entity_id = str(raw["entity_id"])
            delta = _to_float(raw["delta_score"])
            agg = score_agg_by_entity.get(entity_id)
            if agg is None:
                score_agg_by_entity[entity_id] = [delta, 1.0]
            else:
                agg[0] = float(agg[0]) + delta
                agg[1] = float(agg[1]) + 1.0
    finally:
        scorer_writer.close()
    if scorer_rows <= 0:
        raise ValueError(f"Input table is empty: {Path(scorer_output_path)}")

    registry_writer = _TableRowWriter(
        path=registry_path,
        columns=REGISTRY_COLUMNS,
        output_format=output_format,
        chunk_rows=_DEFAULT_STREAM_CHUNK_ROWS,
    )
    registry_rows = 0
    try:
        for raw in _iter_table_rows(
            ccre_state_path,
            CCRE_STATE_COLUMNS,
            input_format=ccre_state_format,
        ):
            ccre_id = str(raw["ccre_id"])
            haplotype_id = str(raw["haplotype_id"])
            entity_id = _entity_id(ccre_id, haplotype_id)
            reason = _safe_reason_map(raw.get("state_reason"))

            evidence = assays_by_entity.get(
                entity_id,
                {"hit_count": 0, "total_labels": 0, "assay_types": set(), "study_ids": set()},
            )
            assay_types = sorted(cast for cast in (evidence.get("assay_types", set()) or set()) if cast is not None)
            study_ids = sorted(cast for cast in (evidence.get("study_ids", set()) or set()) if cast is not None)

            score_agg = score_agg_by_entity.get(entity_id, [0.0, 0.0])
            score_sum = float(score_agg[0])
            score_count = float(score_agg[1])
            ranking_score = score_sum / score_count if score_count > 0 else 0.0

            registry_writer.write_row(
                {
                    "entity_id": entity_id,
                    "source_anchor_ccre": ccre_id,
                    "haplotype_id": haplotype_id,
                    "state_class": str(raw["state_class"]),
                    "ref_chr": str(reason.get("ref_chr", "")),
                    "ref_start": int(reason.get("ref_start", 0) or 0),
                    "ref_end": int(reason.get("ref_end", 0) or 0),
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
                    "evidence_summary": json.dumps(
                        {
                            "hit_count": int(evidence.get("hit_count", 0) or 0),
                            "total_labels": int(evidence.get("total_labels", 0) or 0),
                            "assay_types": assay_types,
                            "study_ids": study_ids,
                        },
                        sort_keys=True,
                    ),
                    "ranking_score": float(ranking_score),
                    "qc_flag": str(raw["qc_flag"]),
                }
            )
            registry_rows += 1
    finally:
        registry_writer.close()
    if registry_rows <= 0:
        raise ValueError(f"Input table is empty: {Path(ccre_state_path)}")

    manifest = {
        "output_format": output_format,
        "files": {
            "polymorphic_ccre_registry": str(registry_path.resolve()),
            "replacement_candidates": str(replacement_path.resolve()),
            "scorer_outputs": str(scorer_path.resolve()),
            "validation_links": str(validation_path.resolve()),
        },
        "row_counts": {
            "polymorphic_ccre_registry": int(registry_rows),
            "replacement_candidates": int(replacement_rows),
            "scorer_outputs": int(scorer_rows),
            "validation_links": int(validation_rows),
        },
    }
    (out_dir / "registry_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    return RegistryBuildResult(output_dir=out_dir, output_format=output_format, registry_rows=int(registry_rows))
