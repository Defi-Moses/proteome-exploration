"""Ablation evaluation for disagreement features."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

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
    feature_matrix = _read_table(feature_matrix_path, FEATURE_MATRIX_COLUMNS, input_format=feature_matrix_format)
    validation_link = _read_table(validation_link_path, VALIDATION_LINK_COLUMNS, input_format=validation_link_format)
    disagreement = _read_table(disagreement_path, DISAGREEMENT_COLUMNS, input_format=disagreement_format)

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
