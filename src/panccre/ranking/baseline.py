"""Cheap baseline ranker training and evaluation."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

import numpy as np
import pandas as pd

from panccre.evaluation import VALIDATION_LINK_COLUMNS
from panccre.features import FEATURE_MATRIX_COLUMNS


@dataclass(frozen=True)
class RankingEvaluationResult:
    report_path: Path
    scores_path: Path


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


def _sigmoid(values: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-values))


def _prepare_training_frame(feature_matrix: pd.DataFrame, validation_link: pd.DataFrame) -> pd.DataFrame:
    state_features = feature_matrix[feature_matrix["entity_type"] == "ref_state"].copy()
    if state_features.empty:
        raise ValueError("No ref_state rows found in feature_matrix")

    wide = state_features.pivot_table(
        index="entity_id",
        columns="feature_name",
        values="feature_value",
        aggfunc="first",
        fill_value=0.0,
    ).reset_index()

    validation = validation_link.copy()
    label_map = {"hit": 1.0, "non-hit": 0.0}
    validation["label_binary"] = validation["label"].map(label_map)
    if validation["label_binary"].isna().any():
        raise ValueError("validation_link contains unsupported label values")

    merged = validation.merge(wide, on="entity_id", how="inner")
    if merged.empty:
        raise ValueError("No overlapping entities between feature_matrix and validation_link")

    return merged


def _fit_linear_baseline(train: pd.DataFrame, feature_columns: list[str], ridge_lambda: float = 1e-3) -> dict[str, object]:
    x = train[feature_columns].to_numpy(dtype=float)
    y = train["label_binary"].to_numpy(dtype=float)

    mean = x.mean(axis=0)
    std = x.std(axis=0)
    std[std == 0] = 1.0

    x_norm = (x - mean) / std
    x_aug = np.hstack([np.ones((x_norm.shape[0], 1)), x_norm])

    ident = np.eye(x_aug.shape[1])
    ident[0, 0] = 0.0

    weights = np.linalg.pinv(x_aug.T @ x_aug + ridge_lambda * ident) @ (x_aug.T @ y)

    return {
        "feature_columns": feature_columns,
        "mean": mean.tolist(),
        "std": std.tolist(),
        "weights": weights.tolist(),
    }


def _predict_linear(model: dict[str, object], frame: pd.DataFrame) -> np.ndarray:
    feature_columns = model["feature_columns"]
    x = frame[feature_columns].to_numpy(dtype=float)

    mean = np.array(model["mean"], dtype=float)
    std = np.array(model["std"], dtype=float)
    weights = np.array(model["weights"], dtype=float)

    x_norm = (x - mean) / std
    x_aug = np.hstack([np.ones((x_norm.shape[0], 1)), x_norm])
    logits = x_aug @ weights
    return _sigmoid(logits)


def _predict_naive(frame: pd.DataFrame) -> np.ndarray:
    def col(name: str) -> np.ndarray:
        if name not in frame.columns:
            return np.zeros(frame.shape[0], dtype=float)
        return frame[name].to_numpy(dtype=float)

    score = (
        0.60 * col("state_is_absent")
        + 0.45 * col("state_is_fractured")
        + 0.25 * col("state_is_diverged")
        + 0.20 * col("state_is_duplicated")
        + 0.30 * (1.0 - col("seq_identity"))
        + 0.20 * (1.0 - col("coverage_frac"))
    )
    return np.clip(score, 0.0, 1.0)


def _hit_rate_at_k(frame: pd.DataFrame, score_column: str, k: int) -> float:
    if frame.empty:
        return 0.0
    k_eff = min(k, frame.shape[0])
    top = frame.nlargest(k_eff, score_column)
    hits = float((top["label_binary"] == 1.0).sum())
    return hits / float(k_eff)


def _precision_recall_auc(frame: pd.DataFrame, score_column: str) -> float:
    if frame.empty:
        return 0.0

    ranked = frame.sort_values(score_column, ascending=False).reset_index(drop=True)
    total_pos = float((ranked["label_binary"] == 1.0).sum())
    if total_pos == 0.0:
        return 0.0

    tp = 0.0
    fp = 0.0
    recall_prev = 0.0
    precision_prev = 1.0
    auc = 0.0

    for _, row in ranked.iterrows():
        if float(row["label_binary"]) == 1.0:
            tp += 1.0
        else:
            fp += 1.0

        recall = tp / total_pos
        precision = tp / max(tp + fp, 1.0)

        auc += (recall - recall_prev) * (precision + precision_prev) / 2.0
        recall_prev = recall
        precision_prev = precision

    return float(auc)


def evaluate_cheap_baselines(
    feature_matrix: pd.DataFrame,
    validation_link: pd.DataFrame,
    *,
    k_values: list[int] | None = None,
) -> tuple[dict[str, object], pd.DataFrame]:
    """Train/evaluate cheap baseline vs naive baseline on holdout splits."""
    if list(feature_matrix.columns) != FEATURE_MATRIX_COLUMNS:
        raise ValueError("feature_matrix contract mismatch")
    if list(validation_link.columns) != VALIDATION_LINK_COLUMNS:
        raise ValueError("validation_link contract mismatch")

    k_values = k_values or [10, 25, 50, 100]

    frame = _prepare_training_frame(feature_matrix, validation_link)
    train = frame[frame["holdout_group"] == "train"].copy()
    test = frame[frame["holdout_group"] == "test"].copy()

    if train.empty or test.empty:
        raise ValueError("validation_link must include both train and test holdout_group rows")

    protected = {
        "entity_id",
        "entity_type",
        "study_id",
        "assay_type",
        "label",
        "effect_size",
        "cell_context",
        "publication_year",
        "holdout_group",
        "label_binary",
    }
    feature_columns = [c for c in frame.columns if c not in protected]

    model = _fit_linear_baseline(train, feature_columns)

    test = test.copy()
    test["score_naive"] = _predict_naive(test)
    test["score_cheap_linear"] = _predict_linear(model, test)

    metrics: dict[str, object] = {
        "dataset": {
            "train_rows": int(train.shape[0]),
            "test_rows": int(test.shape[0]),
            "feature_count": len(feature_columns),
        },
        "top_k": {},
        "pr_auc": {},
        "model": {
            "feature_columns": feature_columns,
            "weights": model["weights"],
        },
    }

    for k in k_values:
        metrics["top_k"][str(k)] = {
            "naive": _hit_rate_at_k(test, "score_naive", k),
            "cheap_linear": _hit_rate_at_k(test, "score_cheap_linear", k),
        }

    metrics["pr_auc"] = {
        "naive": _precision_recall_auc(test, "score_naive"),
        "cheap_linear": _precision_recall_auc(test, "score_cheap_linear"),
    }

    score_view = test[
        [
            "entity_id",
            "label",
            "holdout_group",
            "score_naive",
            "score_cheap_linear",
        ]
    ].sort_values("score_cheap_linear", ascending=False)

    return metrics, score_view


def run_ranking_evaluation(
    *,
    feature_matrix_path: str | Path,
    validation_link_path: str | Path,
    report_output_path: str | Path,
    scores_output_path: str | Path,
    feature_matrix_format: str | None = None,
    validation_link_format: str | None = None,
) -> RankingEvaluationResult:
    feature_matrix = _read_table(feature_matrix_path, FEATURE_MATRIX_COLUMNS, input_format=feature_matrix_format)
    validation_link = _read_table(validation_link_path, VALIDATION_LINK_COLUMNS, input_format=validation_link_format)

    metrics, scores = evaluate_cheap_baselines(feature_matrix, validation_link)

    report_path = Path(report_output_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(metrics, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    scores_path = Path(scores_output_path)
    scores_path.parent.mkdir(parents=True, exist_ok=True)
    scores.to_json(scores_path, orient="records", lines=True)

    return RankingEvaluationResult(report_path=report_path, scores_path=scores_path)
