"""Scorer fanout, shortlist routing, and disagreement features."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import heapq
import json
from pathlib import Path
from typing import Iterator, TextIO

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


class _TableRowWriter:
    def __init__(
        self,
        *,
        path: str | Path,
        columns: list[str],
        output_format: str,
        chunk_rows: int = 20_000,
    ) -> None:
        self.path = Path(path)
        self.columns = columns
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
            self._csv_writer = csv.DictWriter(self._csv_handle, fieldnames=self.columns, extrasaction="raise")
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


def _iter_feature_entities(
    *,
    feature_matrix_path: str | Path,
    feature_matrix_format: str | None = None,
) -> Iterator[tuple[str, str, dict[str, float]]]:
    current_entity_id: str | None = None
    current_entity_type: str | None = None
    current_features: dict[str, float] = {}
    saw_rows = False

    for row in _iter_table_rows(feature_matrix_path, FEATURE_MATRIX_COLUMNS, input_format=feature_matrix_format):
        saw_rows = True
        entity_id = str(row["entity_id"])
        entity_type = str(row["entity_type"])
        feature_name = str(row["feature_name"])
        feature_value = float(row["feature_value"])

        if current_entity_id is None:
            current_entity_id = entity_id
            current_entity_type = entity_type

        if entity_id != current_entity_id or entity_type != current_entity_type:
            assert current_entity_id is not None and current_entity_type is not None
            yield current_entity_id, current_entity_type, current_features
            current_entity_id = entity_id
            current_entity_type = entity_type
            current_features = {}

        if feature_name in current_features:
            raise ValueError(
                "feature_matrix contains duplicate feature rows for entity_id/entity_type/feature_name"
            )
        current_features[feature_name] = feature_value

    if not saw_rows:
        raise ValueError(f"Input table is empty: {Path(feature_matrix_path)}")
    if current_entity_id is not None and current_entity_type is not None:
        yield current_entity_id, current_entity_type, current_features


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
    include_entity_types = ("ref_state",)
    heap: list[tuple[float, str, str]] = []

    for entity_id, entity_type, features in _iter_feature_entities(
        feature_matrix_path=feature_matrix_path,
        feature_matrix_format=feature_matrix_format,
    ):
        if entity_type not in include_entity_types:
            continue

        def col(name: str) -> float:
            return float(features.get(name, 0.0))

        priority = (
            0.38 * (1.0 - col("seq_identity"))
            + 0.32 * (1.0 - col("coverage_frac"))
            + 0.22 * col("state_is_absent")
            + 0.17 * col("state_is_fractured")
            + 0.08 * col("state_is_duplicated")
            + 0.05 * col("state_is_diverged")
        )
        priority += 0.01 * _hash_unit(entity_id)

        item = (float(priority), entity_id, entity_type)
        if len(heap) < int(top_n):
            heapq.heappush(heap, item)
        elif item > heap[0]:
            heapq.heapreplace(heap, item)

    if not heap:
        raise ValueError("No entities available for shortlist selection")

    ranked = sorted(heap, reverse=True)
    shortlist_rows: list[dict[str, object]] = []
    for rank_index, (priority, entity_id, entity_type) in enumerate(ranked, start=1):
        shortlist_rows.append(
            {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "priority_score": float(priority),
                "rank": int(rank_index),
                "selected_for_alphagenome": True,
            }
        )

    shortlist = pd.DataFrame(shortlist_rows, columns=SHORTLIST_COLUMNS)
    validate_shortlist(shortlist)

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
    shortlist = _read_table(shortlist_path, SHORTLIST_COLUMNS, input_format=shortlist_format)
    validate_shortlist(shortlist)

    if int(shortlist.shape[0]) > int(max_alphagenome_calls):
        raise ValueError(
            f"AlphaGenome budget exceeded: shortlist={shortlist.shape[0]} max_calls={max_alphagenome_calls}"
        )

    shortlist_ids = set(shortlist["entity_id"].astype(str).tolist())
    active_run_id = run_id or datetime.now(timezone.utc).strftime("run-%Y%m%d%H%M%S")

    out = Path(output_path)
    writer = _TableRowWriter(path=out, columns=SCORER_OUTPUT_COLUMNS, output_format=output_format)
    row_count = 0
    alpha_calls = 0
    try:
        for entity_id, entity_type, features in _iter_feature_entities(
            feature_matrix_path=feature_matrix_path,
            feature_matrix_format=feature_matrix_format,
        ):
            score_input: dict[str, object] = {
                "entity_id": entity_id,
                "entity_type": entity_type,
            }
            score_input.update(features)

            cheap_ref, cheap_alt, cheap_delta, cheap_unc = _score_cheap(score_input)
            writer.write_row(
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
            row_count += 1

            open_ref, open_alt, open_delta, open_unc = _score_open_model(entity_id, cheap_delta)
            writer.write_row(
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
            row_count += 1

            if entity_id in shortlist_ids:
                alpha_ref, alpha_alt, alpha_delta, alpha_unc = _score_alphagenome(entity_id, cheap_delta, open_delta)
                writer.write_row(
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
                row_count += 1
                alpha_calls += 1
    finally:
        writer.close()

    if row_count <= 0:
        raise ValueError("scorer_output must not be empty")

    return ScorerFanoutResult(
        row_count=int(row_count),
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
    out = Path(output_path)
    writer = _TableRowWriter(path=out, columns=DISAGREEMENT_COLUMNS, output_format=output_format)

    current_entity_id: str | None = None
    current_entity_type: str | None = None
    deltas: list[float] = []
    scorers_present: list[str] = []
    row_count = 0

    def emit_current() -> None:
        nonlocal current_entity_id, current_entity_type, deltas, scorers_present, row_count
        if current_entity_id is None or current_entity_type is None:
            return
        values = np.array(deltas, dtype=float)
        var = float(np.var(values)) if values.size > 0 else 0.0
        min_delta = float(np.min(values)) if values.size > 0 else 0.0
        max_delta = float(np.max(values)) if values.size > 0 else 0.0
        signs = {int(np.sign(v)) for v in values if abs(float(v)) > 1e-9}
        sign_disagreement_count = max(0, len(signs) - 1)
        missing_count = max(0, len(DEFAULT_EXPECTED_SCORERS) - len(set(scorers_present)))

        writer.write_row(
            {
                "entity_id": current_entity_id,
                "entity_type": current_entity_type,
                "score_variance": float(var),
                "sign_disagreement_count": float(sign_disagreement_count),
                # Streaming build avoids global scorer rank maps; emit deterministic zero.
                "rank_disagreement_count": 0.0,
                "max_min_delta": float(max_delta - min_delta),
                "missing_scorer_count": float(missing_count),
                "feature_version": feature_version,
            }
        )
        row_count += 1
        current_entity_id = None
        current_entity_type = None
        deltas = []
        scorers_present = []

    try:
        for row in _iter_table_rows(scorer_output_path, SCORER_OUTPUT_COLUMNS, input_format=scorer_output_format):
            entity_id = str(row["entity_id"])
            entity_type = str(row["entity_type"])
            scorer_name = str(row["scorer_name"])
            delta_score = float(row["delta_score"])

            if current_entity_id is None:
                current_entity_id = entity_id
                current_entity_type = entity_type

            if entity_id != current_entity_id or entity_type != current_entity_type:
                emit_current()
                current_entity_id = entity_id
                current_entity_type = entity_type

            deltas.append(delta_score)
            scorers_present.append(scorer_name)

        emit_current()
    finally:
        writer.close()

    if row_count <= 0:
        raise ValueError("disagreement features must not be empty")

    return DisagreementResult(row_count=int(row_count), output_path=out, output_format=output_format)
