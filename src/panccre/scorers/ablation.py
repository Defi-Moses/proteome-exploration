"""Ablation evaluation for disagreement features."""

from __future__ import annotations

import csv
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Iterator, Mapping

import pandas as pd

from panccre.evaluation import VALIDATION_LINK_COLUMNS
from panccre.features import FEATURE_MATRIX_COLUMNS
from panccre.ranking import evaluate_cheap_baselines
from panccre.scorers.fanout import DISAGREEMENT_COLUMNS, disagreement_to_feature_rows


@dataclass(frozen=True)
class AblationResult:
    report_path: Path


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
) -> Iterator[Mapping[str, object]]:
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


def _metric_lift(base: dict[str, object], enriched: dict[str, object]) -> dict[str, object]:
    lift: dict[str, object] = {"top_k": {}, "pr_auc": {}}

    base_top = base.get("top_k", {})
    enriched_top = enriched.get("top_k", {})
    for k, base_val in base_top.items():
        base_score = float(base_val.get("cheap_linear", 0.0))
        enriched_score = float(enriched_top.get(k, {}).get("cheap_linear", 0.0))
        lift["top_k"][str(k)] = enriched_score - base_score

    base_pr = float(base.get("pr_auc", {}).get("cheap_linear", 0.0))
    enriched_pr = float(enriched.get("pr_auc", {}).get("cheap_linear", 0.0))
    lift["pr_auc"] = {"cheap_linear": enriched_pr - base_pr}

    return lift


def run_disagreement_ablation(
    *,
    feature_matrix_path: str | Path,
    validation_link_path: str | Path,
    disagreement_path: str | Path,
    report_output_path: str | Path,
    feature_matrix_format: str | None = None,
    validation_link_format: str | None = None,
    disagreement_format: str | None = None,
) -> AblationResult:
    validation_link = _read_table(validation_link_path, VALIDATION_LINK_COLUMNS, input_format=validation_link_format)
    required_entity_ids = set(validation_link["entity_id"].astype(str).tolist())

    feature_rows: list[dict[str, object]] = []
    for row in _iter_table_rows(feature_matrix_path, FEATURE_MATRIX_COLUMNS, input_format=feature_matrix_format):
        if str(row["entity_type"]) != "ref_state":
            continue
        if str(row["entity_id"]) not in required_entity_ids:
            continue
        feature_rows.append(
            {
                "entity_id": str(row["entity_id"]),
                "entity_type": str(row["entity_type"]),
                "feature_name": str(row["feature_name"]),
                "feature_value": float(row["feature_value"]),
                "feature_version": str(row["feature_version"]),
            }
        )
    feature_matrix = pd.DataFrame(feature_rows, columns=FEATURE_MATRIX_COLUMNS)
    if feature_matrix.empty:
        raise ValueError("No overlapping ref_state features found for validation_link entity_ids")

    disagreement_rows: list[dict[str, object]] = []
    for row in _iter_table_rows(disagreement_path, DISAGREEMENT_COLUMNS, input_format=disagreement_format):
        if str(row["entity_type"]) != "ref_state":
            continue
        if str(row["entity_id"]) not in required_entity_ids:
            continue
        disagreement_rows.append(
            {
                "entity_id": str(row["entity_id"]),
                "entity_type": str(row["entity_type"]),
                "score_variance": float(row["score_variance"]),
                "sign_disagreement_count": float(row["sign_disagreement_count"]),
                "rank_disagreement_count": float(row["rank_disagreement_count"]),
                "max_min_delta": float(row["max_min_delta"]),
                "missing_scorer_count": float(row["missing_scorer_count"]),
                "feature_version": str(row["feature_version"]),
            }
        )
    disagreement = pd.DataFrame(disagreement_rows, columns=DISAGREEMENT_COLUMNS)
    if disagreement.empty:
        raise ValueError("No overlapping disagreement rows found for validation_link entity_ids")

    base_metrics, _ = evaluate_cheap_baselines(feature_matrix, validation_link)

    dis_features = disagreement_to_feature_rows(disagreement)
    enriched = pd.concat([feature_matrix, dis_features], axis=0, ignore_index=True)
    enriched_metrics, _ = evaluate_cheap_baselines(enriched, validation_link)

    report = {
        "base": {
            "top_k": base_metrics.get("top_k", {}),
            "pr_auc": base_metrics.get("pr_auc", {}),
        },
        "with_disagreement": {
            "top_k": enriched_metrics.get("top_k", {}),
            "pr_auc": enriched_metrics.get("pr_auc", {}),
        },
        "lift": _metric_lift(base_metrics, enriched_metrics),
    }

    out = Path(report_output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return AblationResult(report_path=out)
