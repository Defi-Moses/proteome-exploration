"""Build phase-1 reporting bundles, summary tables, and case-study packets."""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from dataclasses import dataclass
import heapq
import json
from pathlib import Path
from typing import Any, Iterator, Mapping

import pandas as pd

from panccre.evaluation import VALIDATION_LINK_COLUMNS
from panccre.registry import REGISTRY_COLUMNS
from panccre.scorers import DISAGREEMENT_COLUMNS, SCORER_OUTPUT_COLUMNS


@dataclass(frozen=True)
class Phase1ReportResult:
    output_dir: Path
    summary_report_path: Path
    top_hits_path: Path
    case_study_dir: Path
    bundle_manifest_path: Path


def _parquet_available() -> bool:
    try:
        pd.io.parquet.get_engine("auto")
        return True
    except Exception:
        return False


def _infer_format(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return "parquet"
    if suffix == ".csv":
        return "csv"
    if suffix in {".jsonl", ".ndjson"}:
        return "jsonl"
    raise ValueError(f"Unsupported table extension: {path}")


def _read_table(path: str | Path, required_columns: list[str], *, input_format: str | None = None) -> pd.DataFrame:
    file_path = Path(path)
    fmt = (input_format or _infer_format(file_path)).lower()

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

    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required columns in {file_path}: {missing}")
    if frame.empty:
        raise ValueError(f"Input table is empty: {file_path}")
    return frame


def _iter_table_rows(
    path: str | Path,
    required_columns: list[str],
    *,
    input_format: str | None = None,
) -> Iterator[dict[str, object]]:
    file_path = Path(path)
    fmt = (input_format or _infer_format(file_path)).lower()

    if fmt == "jsonl":
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                payload = json.loads(text)
                if not isinstance(payload, dict):
                    raise ValueError("JSONL row must decode to object")
                missing = [column for column in required_columns if column not in payload]
                if missing:
                    raise ValueError(f"Missing required columns in {file_path}: {missing}")
                yield payload
        return

    if fmt == "csv":
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise ValueError(f"Missing header row in CSV: {file_path}")
            missing = [column for column in required_columns if column not in reader.fieldnames]
            if missing:
                raise ValueError(f"Missing required columns in {file_path}: {missing}")
            for row in reader:
                yield row
        return

    if fmt == "parquet":
        frame = _read_table(file_path, required_columns, input_format="parquet")
        for record in frame.to_dict(orient="records"):
            yield record
        return

    raise ValueError("input_format must be one of: parquet, csv, jsonl")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _load_json(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def _safe_parse_json_map(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
            if isinstance(value, dict):
                return value
        except Exception:
            return {}
    return {}


def _state_distribution(registry: pd.DataFrame) -> pd.DataFrame:
    total = max(int(registry.shape[0]), 1)
    counts = registry.groupby("state_class", as_index=False).size().rename(columns={"size": "row_count"})
    counts["row_fraction"] = counts["row_count"] / float(total)
    return counts.sort_values(["row_count", "state_class"], ascending=[False, True]).reset_index(drop=True)


def _assay_enrichment_by_state(registry: pd.DataFrame, validation: pd.DataFrame) -> pd.DataFrame:
    merged = registry[["entity_id", "state_class"]].merge(
        validation[["entity_id", "label"]],
        on="entity_id",
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame(
            columns=["state_class", "row_count", "hit_count", "hit_rate", "global_hit_rate", "enrichment_over_global"]
        )

    merged["is_hit"] = (merged["label"].astype(str) == "hit").astype(float)
    global_hit_rate = float(merged["is_hit"].mean())
    grouped = merged.groupby("state_class", as_index=False).agg(
        row_count=("entity_id", "size"),
        hit_count=("is_hit", "sum"),
        hit_rate=("is_hit", "mean"),
    )
    grouped["global_hit_rate"] = global_hit_rate
    grouped["enrichment_over_global"] = grouped["hit_rate"] / max(global_hit_rate, 1e-9)
    return grouped.sort_values(["enrichment_over_global", "row_count"], ascending=[False, False]).reset_index(drop=True)


def _top_k_table(publication_report: dict[str, Any], locus_report: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for holdout_name, payload in (("publication", publication_report), ("locus", locus_report)):
        top_k = payload.get("top_k", {})
        if not isinstance(top_k, dict):
            continue
        for k, metrics in top_k.items():
            metric_map = metrics if isinstance(metrics, dict) else {}
            naive = _to_float(metric_map.get("naive"))
            cheap_linear = _to_float(metric_map.get("cheap_linear"))
            rows.append(
                {
                    "holdout": holdout_name,
                    "k": int(k),
                    "naive_hit_rate": naive,
                    "cheap_linear_hit_rate": cheap_linear,
                    "lift_over_naive": cheap_linear - naive,
                }
            )
    if not rows:
        return pd.DataFrame(columns=["holdout", "k", "naive_hit_rate", "cheap_linear_hit_rate", "lift_over_naive"])
    return pd.DataFrame(rows).sort_values(["holdout", "k"]).reset_index(drop=True)


def _disagreement_vs_hit_probability(disagreement: pd.DataFrame, validation: pd.DataFrame, *, bucket_count: int = 5) -> pd.DataFrame:
    merged = disagreement.merge(validation[["entity_id", "label"]], on="entity_id", how="inner")
    if merged.empty:
        return pd.DataFrame(
            columns=[
                "disagreement_bucket",
                "row_count",
                "hit_rate",
                "avg_disagreement_index",
                "avg_score_variance",
                "avg_sign_disagreement_count",
                "avg_rank_disagreement_count",
            ]
        )

    merged["is_hit"] = (merged["label"].astype(str) == "hit").astype(float)
    merged["disagreement_index"] = (
        merged["score_variance"].astype(float)
        + 0.5 * merged["sign_disagreement_count"].astype(float)
        + 0.2 * merged["rank_disagreement_count"].astype(float)
    )

    unique_count = int(merged["disagreement_index"].nunique())
    q = min(max(unique_count, 1), max(bucket_count, 1))
    if q <= 1:
        merged["disagreement_bucket"] = "Q1"
    else:
        labels = [f"Q{i}" for i in range(1, q + 1)]
        merged["disagreement_bucket"] = pd.qcut(
            merged["disagreement_index"],
            q=q,
            labels=labels,
            duplicates="drop",
        )

    table = (
        merged.groupby("disagreement_bucket", as_index=False, observed=False)
        .agg(
            row_count=("entity_id", "size"),
            hit_rate=("is_hit", "mean"),
            avg_disagreement_index=("disagreement_index", "mean"),
            avg_score_variance=("score_variance", "mean"),
            avg_sign_disagreement_count=("sign_disagreement_count", "mean"),
            avg_rank_disagreement_count=("rank_disagreement_count", "mean"),
        )
        .sort_values("disagreement_bucket")
        .reset_index(drop=True)
    )
    table["disagreement_bucket"] = table["disagreement_bucket"].astype(str)
    return table


def _assay_inventory(validation: pd.DataFrame) -> pd.DataFrame:
    frame = validation.copy()
    frame["is_hit"] = (frame["label"].astype(str) == "hit").astype(float)
    grouped = (
        frame.groupby(["study_id", "assay_type", "cell_context", "publication_year"], as_index=False)
        .agg(
            row_count=("entity_id", "size"),
            hit_count=("is_hit", "sum"),
            hit_rate=("is_hit", "mean"),
        )
        .sort_values(["publication_year", "study_id", "assay_type"])
        .reset_index(drop=True)
    )
    grouped["hit_count"] = grouped["hit_count"].astype(int)
    return grouped


def _scorer_ablation_table(ablation_summary: dict[str, Any] | None) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    if not ablation_summary:
        return pd.DataFrame(columns=["holdout", "metric", "k", "lift"])

    for holdout in ("publication", "locus"):
        holdout_payload = ablation_summary.get(holdout, {})
        if not isinstance(holdout_payload, dict):
            continue

        top_k_payload = holdout_payload.get("top_k", {})
        if isinstance(top_k_payload, dict):
            for k, lift in top_k_payload.items():
                rows.append(
                    {
                        "holdout": holdout,
                        "metric": "top_k_lift",
                        "k": int(k),
                        "lift": _to_float(lift),
                    }
                )

        pr_payload = holdout_payload.get("pr_auc", {})
        if isinstance(pr_payload, dict):
            rows.append(
                {
                    "holdout": holdout,
                    "metric": "pr_auc_lift",
                    "k": None,
                    "lift": _to_float(pr_payload.get("cheap_linear")),
                }
            )

    if not rows:
        return pd.DataFrame(columns=["holdout", "metric", "k", "lift"])
    return pd.DataFrame(rows).sort_values(["holdout", "metric", "k"], na_position="last").reset_index(drop=True)


def _failure_mode_taxonomy(registry: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        registry.groupby(["qc_flag", "state_class"], as_index=False)
        .size()
        .rename(columns={"size": "row_count"})
        .sort_values(["row_count", "qc_flag", "state_class"], ascending=[False, True, True])
        .reset_index(drop=True)
    )
    total = max(int(grouped["row_count"].sum()), 1)
    grouped["row_fraction"] = grouped["row_count"] / float(total)
    return grouped


def _top_ranked_loci(registry: pd.DataFrame, *, top_k: int) -> pd.DataFrame:
    frame = registry.copy()
    frame["ranking_score"] = frame["ranking_score"].apply(_to_float)
    return frame.sort_values(["ranking_score", "entity_id"], ascending=[False, True]).head(top_k).reset_index(drop=True)


def _case_studies(
    *,
    registry_top: pd.DataFrame,
    validation: pd.DataFrame,
    scorer_outputs: pd.DataFrame | None,
    disagreement: pd.DataFrame | None,
    output_dir: Path,
    case_study_count: int,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    disagreement_map: dict[str, dict[str, Any]] = {}
    if disagreement is not None and not disagreement.empty:
        for _, row in disagreement.iterrows():
            disagreement_map[str(row["entity_id"])] = {
                "score_variance": _to_float(row.get("score_variance")),
                "sign_disagreement_count": _to_float(row.get("sign_disagreement_count")),
                "rank_disagreement_count": _to_float(row.get("rank_disagreement_count")),
            }

    scorer_rows: dict[str, list[dict[str, Any]]] = {}
    if scorer_outputs is not None and not scorer_outputs.empty:
        for _, row in scorer_outputs.iterrows():
            entity_id = str(row["entity_id"])
            scorer_rows.setdefault(entity_id, []).append(
                {
                    "scorer_name": str(row["scorer_name"]),
                    "delta_score": _to_float(row["delta_score"]),
                    "uncertainty": _to_float(row["uncertainty"]),
                    "run_id": str(row["run_id"]),
                }
            )

    paths: list[Path] = []
    selected = registry_top.head(max(case_study_count, 1))
    for idx, (_, row) in enumerate(selected.iterrows(), start=1):
        entity_id = str(row["entity_id"])
        anchor = str(row["source_anchor_ccre"])
        packet = {
            "case_rank": idx,
            "entity_id": entity_id,
            "source_anchor_ccre": anchor,
            "haplotype_id": str(row["haplotype_id"]),
            "state_class": str(row["state_class"]),
            "ranking_score": _to_float(row["ranking_score"]),
            "qc_flag": str(row["qc_flag"]),
            "provenance": _safe_parse_json_map(row.get("provenance")),
            "evidence_summary": _safe_parse_json_map(row.get("evidence_summary")),
            "validation_rows": validation.loc[validation["entity_id"] == entity_id].to_dict(orient="records"),
            "scorer_rows": scorer_rows.get(entity_id, []),
            "disagreement": disagreement_map.get(entity_id, {}),
        }

        case_path = output_dir / f"case_{idx:03d}_{anchor}.json"
        _write_json(case_path, packet)
        paths.append(case_path)

    return paths


def _bundle_manifest_paths(output_dir: Path) -> list[Path]:
    files: list[Path] = []
    for path in output_dir.rglob("*"):
        if path.is_file():
            files.append(path)
    return sorted(files)


def _validation_entity_stats(validation: pd.DataFrame) -> dict[str, tuple[int, int]]:
    stats: dict[str, tuple[int, int]] = {}
    grouped = validation.groupby("entity_id", sort=False)
    for entity_id, group in grouped:
        hit_count = int((group["label"].astype(str) == "hit").sum())
        total = int(group.shape[0])
        stats[str(entity_id)] = (total, hit_count)
    return stats


def _state_distribution_from_counts(state_counts: Counter[str]) -> pd.DataFrame:
    total = max(int(sum(state_counts.values())), 1)
    rows = [
        {
            "state_class": state_class,
            "row_count": int(count),
            "row_fraction": float(count) / float(total),
        }
        for state_class, count in state_counts.items()
    ]
    if not rows:
        return pd.DataFrame(columns=["state_class", "row_count", "row_fraction"])
    return pd.DataFrame(rows).sort_values(["row_count", "state_class"], ascending=[False, True]).reset_index(drop=True)


def _assay_enrichment_from_counts(
    state_counts: dict[str, tuple[int, int]],
    *,
    global_total: int,
    global_hits: int,
) -> pd.DataFrame:
    if global_total <= 0:
        return pd.DataFrame(
            columns=["state_class", "row_count", "hit_count", "hit_rate", "global_hit_rate", "enrichment_over_global"]
        )
    global_hit_rate = float(global_hits) / float(global_total)
    rows = []
    for state_class, (row_count, hit_count) in state_counts.items():
        hit_rate = float(hit_count) / float(max(row_count, 1))
        rows.append(
            {
                "state_class": str(state_class),
                "row_count": int(row_count),
                "hit_count": int(hit_count),
                "hit_rate": hit_rate,
                "global_hit_rate": global_hit_rate,
                "enrichment_over_global": hit_rate / max(global_hit_rate, 1e-9),
            }
        )
    if not rows:
        return pd.DataFrame(
            columns=["state_class", "row_count", "hit_count", "hit_rate", "global_hit_rate", "enrichment_over_global"]
        )
    return pd.DataFrame(rows).sort_values(["enrichment_over_global", "row_count"], ascending=[False, False]).reset_index(drop=True)


def _failure_mode_from_counts(failure_counts: Counter[tuple[str, str]]) -> pd.DataFrame:
    total = max(int(sum(failure_counts.values())), 1)
    rows = [
        {
            "qc_flag": str(qc_flag),
            "state_class": str(state_class),
            "row_count": int(count),
            "row_fraction": float(count) / float(total),
        }
        for (qc_flag, state_class), count in failure_counts.items()
    ]
    if not rows:
        return pd.DataFrame(columns=["qc_flag", "state_class", "row_count", "row_fraction"])
    return pd.DataFrame(rows).sort_values(["row_count", "qc_flag", "state_class"], ascending=[False, True, True]).reset_index(
        drop=True
    )


def build_phase1_report_bundle(
    *,
    registry_path: str | Path,
    validation_links_path: str | Path,
    publication_ranking_report_path: str | Path,
    locus_ranking_report_path: str | Path,
    output_dir: str | Path,
    disagreement_features_path: str | Path | None = None,
    scorer_outputs_path: str | Path | None = None,
    ablation_summary_path: str | Path | None = None,
    top_hits_k: int = 100,
    case_study_count: int = 3,
    registry_format: str | None = None,
    validation_links_format: str | None = None,
    disagreement_features_format: str | None = None,
    scorer_outputs_format: str | None = None,
) -> Phase1ReportResult:
    validation = _read_table(validation_links_path, VALIDATION_LINK_COLUMNS, input_format=validation_links_format)
    publication_report = _load_json(publication_ranking_report_path)
    locus_report = _load_json(locus_ranking_report_path)
    validation_stats = _validation_entity_stats(validation)
    validation_entity_ids = set(validation_stats.keys())

    state_counts: Counter[str] = Counter()
    failure_counts: Counter[tuple[str, str]] = Counter()
    validation_by_state_counts: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    validation_total_labels = 0
    validation_total_hits = 0

    top_heap: list[tuple[float, str, dict[str, object]]] = []
    registry_row_count = 0
    for raw in _iter_table_rows(registry_path, REGISTRY_COLUMNS, input_format=registry_format):
        registry_row_count += 1
        state_class = str(raw["state_class"])
        qc_flag = str(raw["qc_flag"])
        entity_id = str(raw["entity_id"])
        ranking_score = _to_float(raw["ranking_score"])

        state_counts[state_class] += 1
        failure_counts[(qc_flag, state_class)] += 1

        validation_counts = validation_stats.get(entity_id)
        if validation_counts is not None:
            total_labels, hit_count = validation_counts
            validation_by_state_counts[state_class][0] += int(total_labels)
            validation_by_state_counts[state_class][1] += int(hit_count)
            validation_total_labels += int(total_labels)
            validation_total_hits += int(hit_count)

        normalized_row = {column: raw.get(column) for column in REGISTRY_COLUMNS}
        normalized_row["ranking_score"] = float(ranking_score)
        heap_item = (float(ranking_score), entity_id, normalized_row)
        if len(top_heap) < max(int(top_hits_k), 1):
            heapq.heappush(top_heap, heap_item)
        else:
            min_score, min_entity, _ = top_heap[0]
            if (ranking_score, entity_id) > (min_score, min_entity):
                heapq.heapreplace(top_heap, heap_item)

    if registry_row_count <= 0:
        raise ValueError(f"Input table is empty: {Path(registry_path)}")

    top_rows = [item[2] for item in top_heap]
    top_hits = pd.DataFrame(top_rows, columns=REGISTRY_COLUMNS)
    if not top_hits.empty:
        top_hits = top_hits.sort_values(["ranking_score", "entity_id"], ascending=[False, True]).reset_index(drop=True)

    state_distribution = _state_distribution_from_counts(state_counts)
    assay_enrichment = _assay_enrichment_from_counts(
        {state_class: (counts[0], counts[1]) for state_class, counts in validation_by_state_counts.items()},
        global_total=validation_total_labels,
        global_hits=validation_total_hits,
    )
    failure_modes = _failure_mode_from_counts(failure_counts)

    disagreement: pd.DataFrame | None = None
    if disagreement_features_path is not None and Path(disagreement_features_path).exists():
        disagreement_rows: list[dict[str, object]] = []
        for raw in _iter_table_rows(
            disagreement_features_path,
            DISAGREEMENT_COLUMNS,
            input_format=disagreement_features_format,
        ):
            entity_id = str(raw["entity_id"])
            if entity_id not in validation_entity_ids:
                continue
            disagreement_rows.append(
                {
                    "entity_id": entity_id,
                    "entity_type": str(raw["entity_type"]),
                    "score_variance": _to_float(raw["score_variance"]),
                    "sign_disagreement_count": _to_float(raw["sign_disagreement_count"]),
                    "rank_disagreement_count": _to_float(raw["rank_disagreement_count"]),
                    "max_min_delta": _to_float(raw["max_min_delta"]),
                    "missing_scorer_count": _to_float(raw["missing_scorer_count"]),
                    "feature_version": str(raw["feature_version"]),
                }
            )
        if disagreement_rows:
            disagreement = pd.DataFrame(disagreement_rows, columns=DISAGREEMENT_COLUMNS)

    scorer_outputs: pd.DataFrame | None = None
    if scorer_outputs_path is not None and Path(scorer_outputs_path).exists():
        top_entity_ids = set(top_hits["entity_id"].astype(str).tolist()) if not top_hits.empty else set()
        if top_entity_ids:
            scorer_rows: list[dict[str, object]] = []
            for raw in _iter_table_rows(
                scorer_outputs_path,
                SCORER_OUTPUT_COLUMNS,
                input_format=scorer_outputs_format,
            ):
                entity_id = str(raw["entity_id"])
                if entity_id not in top_entity_ids:
                    continue
                scorer_rows.append(
                    {
                        "entity_id": entity_id,
                        "entity_type": str(raw["entity_type"]),
                        "scorer_name": str(raw["scorer_name"]),
                        "assay_proxy": str(raw["assay_proxy"]),
                        "context_group": str(raw["context_group"]),
                        "ref_score": _to_float(raw["ref_score"]),
                        "alt_score": _to_float(raw["alt_score"]),
                        "delta_score": _to_float(raw["delta_score"]),
                        "uncertainty": _to_float(raw["uncertainty"]),
                        "run_id": str(raw["run_id"]),
                    }
                )
            if scorer_rows:
                scorer_outputs = pd.DataFrame(scorer_rows, columns=SCORER_OUTPUT_COLUMNS)

    ablation_summary: dict[str, Any] | None = None
    if ablation_summary_path is not None and Path(ablation_summary_path).exists():
        ablation_summary = _load_json(ablation_summary_path)

    out_dir = Path(output_dir)
    figures_dir = out_dir / "figures"
    tables_dir = out_dir / "tables"
    case_study_dir = out_dir / "case_studies"
    figures_dir.mkdir(parents=True, exist_ok=True)
    tables_dir.mkdir(parents=True, exist_ok=True)
    case_study_dir.mkdir(parents=True, exist_ok=True)

    state_distribution_path = figures_dir / "state_class_distribution.csv"
    state_distribution.to_csv(state_distribution_path, index=False)

    assay_enrichment_path = figures_dir / "assay_enrichment_by_state_class.csv"
    assay_enrichment.to_csv(assay_enrichment_path, index=False)

    top_k_table = _top_k_table(publication_report, locus_report)
    top_k_table_path = figures_dir / "top_k_hit_rate_vs_baseline.csv"
    top_k_table.to_csv(top_k_table_path, index=False)

    disagreement_path = figures_dir / "disagreement_vs_hit_probability.csv"
    if disagreement is not None:
        disagreement_hit = _disagreement_vs_hit_probability(disagreement, validation)
        disagreement_hit.to_csv(disagreement_path, index=False)
    else:
        pd.DataFrame(columns=["disagreement_bucket", "row_count", "hit_rate"]).to_csv(disagreement_path, index=False)

    top_hits_path = tables_dir / "top_100_ranked_loci.csv"
    top_hits.to_csv(top_hits_path, index=False)

    assay_inventory = _assay_inventory(validation)
    assay_inventory_path = tables_dir / "assay_inventory.csv"
    assay_inventory.to_csv(assay_inventory_path, index=False)

    scorer_ablation = _scorer_ablation_table(ablation_summary)
    scorer_ablation_path = tables_dir / "scorer_ablation_summary.csv"
    scorer_ablation.to_csv(scorer_ablation_path, index=False)

    failure_modes_path = tables_dir / "failure_mode_taxonomy.csv"
    failure_modes.to_csv(failure_modes_path, index=False)

    case_paths = _case_studies(
        registry_top=top_hits,
        validation=validation,
        scorer_outputs=scorer_outputs,
        disagreement=disagreement,
        output_dir=case_study_dir,
        case_study_count=max(int(case_study_count), 1),
    )

    summary_report_path = out_dir / "report.md"
    report_lines = [
        "# Phase-1 Report Bundle",
        "",
        "## Scope",
        "- Context: immune_hematopoietic",
        f"- Registry rows: {int(registry_row_count)}",
        f"- Validation rows: {int(validation.shape[0])}",
        "",
        "## Required Figures",
        f"- State class distribution: {state_distribution_path}",
        f"- Assay enrichment by state class: {assay_enrichment_path}",
        f"- Top-k hit rate vs baselines: {top_k_table_path}",
        f"- Disagreement vs hit probability: {disagreement_path}",
        "",
        "## Required Tables",
        f"- Top ranked loci: {top_hits_path}",
        f"- Assay inventory: {assay_inventory_path}",
        f"- Scorer ablation summary: {scorer_ablation_path}",
        f"- Failure-mode taxonomy: {failure_modes_path}",
        "",
        "## Case Studies",
    ]
    report_lines.extend([f"- {path}" for path in case_paths])
    summary_report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    bundle_manifest_path = out_dir / "bundle_manifest.json"
    bundle_files = [str(path.resolve()) for path in _bundle_manifest_paths(out_dir) if path != bundle_manifest_path]
    _write_json(
        bundle_manifest_path,
        {
            "summary_report": str(summary_report_path.resolve()),
            "top_hits_table": str(top_hits_path.resolve()),
            "case_study_dir": str(case_study_dir.resolve()),
            "files": bundle_files,
        },
    )

    return Phase1ReportResult(
        output_dir=out_dir,
        summary_report_path=summary_report_path,
        top_hits_path=top_hits_path,
        case_study_dir=case_study_dir,
        bundle_manifest_path=bundle_manifest_path,
    )
