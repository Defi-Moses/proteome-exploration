"""Scorer fanout, shortlist routing, and disagreement features."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from panccre.features import FEATURE_MATRIX_COLUMNS

SHORTLIST_COLUMNS = [
    "entity_id",
    "entity_type",
    "priority_score",
    "rank",
    "selected_for_alphagenome",
]

SCORER_OUTPUT_COLUMNS = [
    "entity_id",
    "entity_type",
    "scorer_name",
    "assay_proxy",
    "context_group",
    "ref_score",
    "alt_score",
    "delta_score",
    "uncertainty",
    "run_id",
]

DISAGREEMENT_COLUMNS = [
    "entity_id",
    "entity_type",
    "score_variance",
    "sign_disagreement_count",
    "rank_disagreement_count",
    "max_min_delta",
    "missing_scorer_count",
    "feature_version",
]

DEFAULT_EXPECTED_SCORERS = ("cheap_baseline", "ntv2_embedding", "alphagenome")


@dataclass(frozen=True)
class ShortlistResult:
    row_count: int
    output_path: Path
    output_format: str


@dataclass(frozen=True)
class ScorerFanoutResult:
    row_count: int
    output_path: Path
    output_format: str
    alphagenome_calls: int


@dataclass(frozen=True)
class DisagreementResult:
    row_count: int
    output_path: Path
    output_format: str


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


def _hash_unit(value: str) -> float:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:12], 16) / float(16**12 - 1)


def _wide_features(feature_matrix: pd.DataFrame) -> pd.DataFrame:
    wide = (
        feature_matrix.pivot_table(
            index=["entity_id", "entity_type"],
            columns="feature_name",
            values="feature_value",
            aggfunc="first",
            fill_value=0.0,
        )
        .reset_index()
        .rename_axis(columns=None)
    )
    if wide.empty:
        raise ValueError("No rows available after pivoting feature matrix")
    return wide


def build_shortlist(feature_matrix: pd.DataFrame, *, top_n: int, include_entity_types: tuple[str, ...] = ("ref_state",)) -> pd.DataFrame:
    if list(feature_matrix.columns) != FEATURE_MATRIX_COLUMNS:
        raise ValueError("feature_matrix contract mismatch")

    wide = _wide_features(feature_matrix)
    wide = wide[wide["entity_type"].isin(include_entity_types)].copy()
    if wide.empty:
        raise ValueError("No entities available for shortlist selection")

    def col(name: str) -> pd.Series:
        if name in wide.columns:
            return wide[name]
        return pd.Series(0.0, index=wide.index)

    wide["priority_score"] = (
        0.38 * (1.0 - col("seq_identity"))
        + 0.32 * (1.0 - col("coverage_frac"))
        + 0.22 * col("state_is_absent")
        + 0.17 * col("state_is_fractured")
        + 0.08 * col("state_is_duplicated")
        + 0.05 * col("state_is_diverged")
    )

    wide["priority_score"] = wide["priority_score"] + wide["entity_id"].apply(lambda v: 0.01 * _hash_unit(str(v)))
    wide = wide.sort_values("priority_score", ascending=False).reset_index(drop=True)

    top_n_eff = min(top_n, int(wide.shape[0]))
    shortlist = wide.head(top_n_eff)[["entity_id", "entity_type", "priority_score"]].copy()
    shortlist["rank"] = np.arange(1, shortlist.shape[0] + 1)
    shortlist["selected_for_alphagenome"] = True

    shortlist = shortlist[SHORTLIST_COLUMNS]
    validate_shortlist(shortlist)
    return shortlist


def validate_shortlist(frame: pd.DataFrame) -> None:
    if list(frame.columns) != SHORTLIST_COLUMNS:
        raise ValueError(f"shortlist column contract mismatch: expected={SHORTLIST_COLUMNS} actual={list(frame.columns)}")
    if frame.empty:
        raise ValueError("shortlist must not be empty")
    if frame["entity_id"].duplicated().any():
        raise ValueError("shortlist contains duplicate entity_id rows")


def run_shortlist_build(
    *,
    feature_matrix_path: str | Path,
    output_path: str | Path,
    top_n: int,
    output_format: str = "jsonl",
    feature_matrix_format: str | None = None,
) -> ShortlistResult:
    feature_matrix = _read_table(feature_matrix_path, FEATURE_MATRIX_COLUMNS, input_format=feature_matrix_format)
    shortlist = build_shortlist(feature_matrix, top_n=top_n)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    _write_table(shortlist, out, output_format)

    return ShortlistResult(row_count=int(shortlist.shape[0]), output_path=out, output_format=output_format)


def _score_cheap(row: pd.Series) -> tuple[float, float, float, float]:
    if row["entity_type"] == "ref_state":
        seq_identity = float(row.get("seq_identity", 1.0))
        coverage = float(row.get("coverage_frac", 1.0))
        delta = (
            0.75 * (1.0 - seq_identity)
            + 0.65 * (1.0 - coverage)
            + 0.90 * float(row.get("state_is_absent", 0.0))
            + 0.55 * float(row.get("state_is_fractured", 0.0))
            + 0.35 * float(row.get("state_is_duplicated", 0.0))
            + 0.25 * float(row.get("state_is_diverged", 0.0))
        )
    else:
        delta = (
            0.08 * float(row.get("seq_len", 0.0)) / 250.0
            + 0.45 * float(row.get("gc_content", 0.0))
            + 0.06 * float(row.get("motif_count", 0.0))
            + 0.08 * (1.0 - min(float(row.get("nearest_gene_distance", 10000.0)) / 50000.0, 1.0))
        )

    ref_score = 0.5
    alt_score = ref_score + delta
    uncertainty = max(0.02, 0.20 - min(delta, 0.15))
    return ref_score, alt_score, delta, uncertainty


def _score_open_model(entity_id: str, cheap_delta: float) -> tuple[float, float, float, float]:
    jitter = (_hash_unit(entity_id + "|open") - 0.5) * 0.14
    ref_score = 0.48 + jitter / 4.0
    delta = 0.72 * cheap_delta + jitter
    alt_score = ref_score + delta
    uncertainty = 0.12 + abs(jitter) * 0.4
    return ref_score, alt_score, delta, uncertainty


def _score_alphagenome(entity_id: str, cheap_delta: float, open_delta: float) -> tuple[float, float, float, float]:
    jitter = (_hash_unit(entity_id + "|alpha") - 0.5) * 0.10
    ref_score = 0.50 + jitter / 6.0
    delta = 0.52 * cheap_delta + 0.62 * open_delta + jitter
    alt_score = ref_score + delta
    uncertainty = 0.07 + abs(jitter) * 0.25
    return ref_score, alt_score, delta, uncertainty


def validate_scorer_output(frame: pd.DataFrame) -> None:
    if list(frame.columns) != SCORER_OUTPUT_COLUMNS:
        raise ValueError(
            "scorer_output column contract mismatch: "
            f"expected={SCORER_OUTPUT_COLUMNS} actual={list(frame.columns)}"
        )
    if frame.empty:
        raise ValueError("scorer_output must not be empty")


def run_scorer_fanout(
    *,
    feature_matrix_path: str | Path,
    shortlist_path: str | Path,
    output_path: str | Path,
    context_group: str,
    max_alphagenome_calls: int,
    output_format: str = "jsonl",
    feature_matrix_format: str | None = None,
    shortlist_format: str | None = None,
    run_id: str | None = None,
) -> ScorerFanoutResult:
    feature_matrix = _read_table(feature_matrix_path, FEATURE_MATRIX_COLUMNS, input_format=feature_matrix_format)
    shortlist = _read_table(shortlist_path, SHORTLIST_COLUMNS, input_format=shortlist_format)
    validate_shortlist(shortlist)

    if int(shortlist.shape[0]) > int(max_alphagenome_calls):
        raise ValueError(
            f"AlphaGenome budget exceeded: shortlist={shortlist.shape[0]} max_calls={max_alphagenome_calls}"
        )

    wide = _wide_features(feature_matrix)
    shortlist_ids = set(shortlist["entity_id"].astype(str).tolist())
    active_run_id = run_id or datetime.now(timezone.utc).strftime("run-%Y%m%d%H%M%S")

    rows: list[dict[str, object]] = []
    for _, row in wide.iterrows():
        entity_id = str(row["entity_id"])
        entity_type = str(row["entity_type"])

        cheap_ref, cheap_alt, cheap_delta, cheap_unc = _score_cheap(row)
        rows.append(
            {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "scorer_name": "cheap_baseline",
                "assay_proxy": "mpra_like",
                "context_group": context_group,
                "ref_score": float(cheap_ref),
                "alt_score": float(cheap_alt),
                "delta_score": float(cheap_delta),
                "uncertainty": float(cheap_unc),
                "run_id": active_run_id,
            }
        )

        open_ref, open_alt, open_delta, open_unc = _score_open_model(entity_id, cheap_delta)
        rows.append(
            {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "scorer_name": "ntv2_embedding",
                "assay_proxy": "mpra_like",
                "context_group": context_group,
                "ref_score": float(open_ref),
                "alt_score": float(open_alt),
                "delta_score": float(open_delta),
                "uncertainty": float(open_unc),
                "run_id": active_run_id,
            }
        )

        if entity_id in shortlist_ids:
            alpha_ref, alpha_alt, alpha_delta, alpha_unc = _score_alphagenome(entity_id, cheap_delta, open_delta)
            rows.append(
                {
                    "entity_id": entity_id,
                    "entity_type": entity_type,
                    "scorer_name": "alphagenome",
                    "assay_proxy": "mpra_like",
                    "context_group": context_group,
                    "ref_score": float(alpha_ref),
                    "alt_score": float(alpha_alt),
                    "delta_score": float(alpha_delta),
                    "uncertainty": float(alpha_unc),
                    "run_id": active_run_id,
                }
            )

    output = pd.DataFrame(rows, columns=SCORER_OUTPUT_COLUMNS).sort_values(
        ["entity_id", "scorer_name"],
        ascending=[True, True],
    )
    validate_scorer_output(output)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    _write_table(output, out, output_format)

    alpha_calls = int((output["scorer_name"] == "alphagenome").sum())
    return ScorerFanoutResult(
        row_count=int(output.shape[0]),
        output_path=out,
        output_format=output_format,
        alphagenome_calls=alpha_calls,
    )


def _rank_maps(scorer_output: pd.DataFrame) -> dict[str, dict[str, int]]:
    ranks: dict[str, dict[str, int]] = {}
    for scorer, group in scorer_output.groupby("scorer_name"):
        sorted_group = group.sort_values("delta_score", ascending=False).reset_index(drop=True)
        ranks[str(scorer)] = {
            str(entity_id): int(rank + 1)
            for rank, entity_id in enumerate(sorted_group["entity_id"].tolist())
        }
    return ranks


def build_disagreement_features(
    scorer_output: pd.DataFrame,
    *,
    expected_scorers: tuple[str, ...] = DEFAULT_EXPECTED_SCORERS,
    feature_version: str = "v1",
) -> pd.DataFrame:
    if list(scorer_output.columns) != SCORER_OUTPUT_COLUMNS:
        raise ValueError("scorer_output contract mismatch")

    ranks = _rank_maps(scorer_output)
    threshold = max(5, int(0.1 * scorer_output["entity_id"].nunique()))

    rows: list[dict[str, object]] = []
    grouped = scorer_output.groupby(["entity_id", "entity_type"], sort=False)
    for (entity_id, entity_type), group in grouped:
        deltas = group["delta_score"].astype(float).to_numpy()
        scorers_present = group["scorer_name"].astype(str).tolist()

        var = float(np.var(deltas)) if deltas.size > 0 else 0.0
        min_delta = float(np.min(deltas)) if deltas.size > 0 else 0.0
        max_delta = float(np.max(deltas)) if deltas.size > 0 else 0.0

        signs = {int(np.sign(v)) for v in deltas if abs(v) > 1e-9}
        sign_disagreement_count = max(0, len(signs) - 1)

        entity_ranks = []
        for scorer in scorers_present:
            rank = ranks.get(scorer, {}).get(str(entity_id))
            if rank is not None:
                entity_ranks.append(rank)
        if entity_ranks:
            median_rank = float(np.median(entity_ranks))
            rank_disagreement_count = int(sum(1 for r in entity_ranks if abs(r - median_rank) > threshold))
        else:
            rank_disagreement_count = 0

        missing_count = max(0, len(expected_scorers) - len(set(scorers_present)))

        rows.append(
            {
                "entity_id": str(entity_id),
                "entity_type": str(entity_type),
                "score_variance": var,
                "sign_disagreement_count": float(sign_disagreement_count),
                "rank_disagreement_count": float(rank_disagreement_count),
                "max_min_delta": max_delta - min_delta,
                "missing_scorer_count": float(missing_count),
                "feature_version": feature_version,
            }
        )

    disagreement = pd.DataFrame(rows, columns=DISAGREEMENT_COLUMNS)
    validate_disagreement_features(disagreement)
    return disagreement


def validate_disagreement_features(frame: pd.DataFrame) -> None:
    if list(frame.columns) != DISAGREEMENT_COLUMNS:
        raise ValueError(
            "disagreement column contract mismatch: "
            f"expected={DISAGREEMENT_COLUMNS} actual={list(frame.columns)}"
        )
    if frame.empty:
        raise ValueError("disagreement features must not be empty")
    if frame["entity_id"].duplicated().any():
        raise ValueError("disagreement features contain duplicate entity_id rows")


def disagreement_to_feature_rows(disagreement: pd.DataFrame) -> pd.DataFrame:
    if list(disagreement.columns) != DISAGREEMENT_COLUMNS:
        raise ValueError("disagreement contract mismatch")

    rows: list[dict[str, object]] = []
    for _, row in disagreement.iterrows():
        for feature_name in [
            "score_variance",
            "sign_disagreement_count",
            "rank_disagreement_count",
            "max_min_delta",
            "missing_scorer_count",
        ]:
            rows.append(
                {
                    "entity_id": str(row["entity_id"]),
                    "entity_type": str(row["entity_type"]),
                    "feature_name": f"dis_{feature_name}",
                    "feature_value": float(row[feature_name]),
                    "feature_version": str(row["feature_version"]),
                }
            )

    frame = pd.DataFrame(rows, columns=FEATURE_MATRIX_COLUMNS)
    return frame


def run_disagreement_build(
    *,
    scorer_output_path: str | Path,
    output_path: str | Path,
    output_format: str = "jsonl",
    scorer_output_format: str | None = None,
    feature_version: str = "v1",
) -> DisagreementResult:
    scorer_output = _read_table(scorer_output_path, SCORER_OUTPUT_COLUMNS, input_format=scorer_output_format)
    disagreement = build_disagreement_features(scorer_output, feature_version=feature_version)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    _write_table(disagreement, out, output_format)

    return DisagreementResult(row_count=int(disagreement.shape[0]), output_path=out, output_format=output_format)
