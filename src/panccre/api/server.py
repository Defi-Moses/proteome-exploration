"""FastAPI service for querying registry artifacts."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Optional, Union

from fastapi import FastAPI, HTTPException, Query

# Keep API column contracts local so service startup does not import
# pandas/numpy-heavy pipeline modules.
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

REPLACEMENT_CANDIDATE_COLUMNS = [
    "candidate_id",
    "parent_ccre_id",
    "haplotype_id",
    "window_class",
    "alt_contig",
    "alt_start",
    "alt_end",
    "seq_len",
    "repeat_class",
    "te_family",
    "motif_count",
    "gc_content",
    "nearest_gene",
    "nearest_gene_distance",
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


def _read_table(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return [dict(row) for row in reader]
    if suffix in {".jsonl", ".ndjson"}:
        rows: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rows.append(json.loads(line))
        return rows
    if suffix == ".parquet":
        try:
            import pyarrow.parquet as pq
        except Exception as exc:
            raise RuntimeError("Parquet support unavailable (install pyarrow)") from exc
        table = pq.read_table(path)
        return table.to_pylist()
    raise ValueError(f"Unsupported file extension: {path}")


def _discover_artifact(base_dir: Path, stem: str) -> Path:
    for ext in ["parquet", "jsonl", "csv"]:
        candidate = base_dir / f"{stem}.{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Artifact not found for stem={stem} in {base_dir}")


def _required_registry_stems() -> list[str]:
    return [
        "polymorphic_ccre_registry",
        "replacement_candidates",
        "scorer_outputs",
        "validation_links",
    ]


def _write_csv_placeholder(path: Path, columns: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()


def _safe_row_count(path: Path) -> int:
    try:
        return int(len(_read_table(path)))
    except Exception:
        return 0


def _ensure_registry_placeholders(base_dir: Path) -> None:
    base_dir.mkdir(parents=True, exist_ok=True)

    placeholders: dict[str, list[str]] = {
        "polymorphic_ccre_registry": REGISTRY_COLUMNS,
        "replacement_candidates": REPLACEMENT_CANDIDATE_COLUMNS,
        "scorer_outputs": SCORER_OUTPUT_COLUMNS,
        "validation_links": VALIDATION_LINK_COLUMNS,
    }

    files: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    for stem, columns in placeholders.items():
        try:
            existing = _discover_artifact(base_dir, stem)
            files[stem] = str(existing.resolve())
            row_counts[stem] = _safe_row_count(existing)
            continue
        except FileNotFoundError:
            pass

        placeholder_path = base_dir / f"{stem}.csv"
        _write_csv_placeholder(placeholder_path, columns)
        files[stem] = str(placeholder_path.resolve())
        row_counts[stem] = 0

    manifest_path = base_dir / "registry_manifest.json"
    if not manifest_path.exists():
        payload = {
            "output_format": "csv",
            "files": files,
            "row_counts": row_counts,
            "generated_by": "api-placeholder-bootstrap",
        }
        manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


class RegistryStore:
    def __init__(self, registry_dir: Path) -> None:
        self.registry_dir = registry_dir
        self._cache: dict[str, list[dict[str, Any]]] = {}

    def _load(self, key: str, stem: str) -> list[dict[str, Any]]:
        if key in self._cache:
            return self._cache[key]

        path = _discover_artifact(self.registry_dir, stem)
        rows = _read_table(path)
        self._cache[key] = rows
        return rows

    @property
    def registry(self) -> list[dict[str, Any]]:
        return self._load("registry", "polymorphic_ccre_registry")

    @property
    def candidates(self) -> list[dict[str, Any]]:
        return self._load("candidates", "replacement_candidates")

    @property
    def scorers(self) -> list[dict[str, Any]]:
        return self._load("scorers", "scorer_outputs")

    @property
    def validations(self) -> list[dict[str, Any]]:
        return self._load("validations", "validation_links")

    def downloads(self) -> dict[str, str]:
        manifest_path = self.registry_dir / "registry_manifest.json"
        if manifest_path.exists():
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            files = payload.get("files")
            if isinstance(files, dict):
                return {str(k): str(v) for k, v in files.items()}

        return {
            "polymorphic_ccre_registry": str(_discover_artifact(self.registry_dir, "polymorphic_ccre_registry")),
            "replacement_candidates": str(_discover_artifact(self.registry_dir, "replacement_candidates")),
            "scorer_outputs": str(_discover_artifact(self.registry_dir, "scorer_outputs")),
            "validation_links": str(_discover_artifact(self.registry_dir, "validation_links")),
        }


def create_app(*, registry_dir: Optional[Union[str, Path]] = None) -> FastAPI:
    base_dir = Path(registry_dir or os.environ.get("PANCCRE_REGISTRY_DIR") or (Path.cwd() / "data" / "registry"))
    auto_seed = os.environ.get("PANCCRE_AUTO_SEED_REGISTRY", "1") != "0"
    if auto_seed:
        _ensure_registry_placeholders(base_dir)

    store = RegistryStore(base_dir)
    app = FastAPI(title="pan-ccre-registry", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        missing: list[str] = []
        for stem in _required_registry_stems():
            try:
                _discover_artifact(base_dir, stem)
            except FileNotFoundError:
                missing.append(stem)

        payload: dict[str, Any] = {
            "status": "ok" if not missing else "degraded",
            "registry_dir": str(base_dir.resolve()),
        }
        if missing:
            payload["missing_artifacts"] = missing
            return payload

        payload["counts"] = {
            "registry": len(store.registry),
            "candidates": len(store.candidates),
            "scorer_outputs": len(store.scorers),
            "validation_links": len(store.validations),
        }
        return payload

    @app.get("/ccre/{ccre_id}")
    def get_ccre(ccre_id: str) -> dict[str, Any]:
        hits = [row for row in store.registry if str(row.get("source_anchor_ccre", "")) == ccre_id]
        if not hits:
            raise HTTPException(status_code=404, detail=f"cCRE not found: {ccre_id}")
        hits = sorted(hits, key=lambda row: _to_float(row.get("ranking_score", 0.0)), reverse=True)
        return {"ccre_id": ccre_id, "rows": hits}

    @app.get("/candidate/{candidate_id}")
    def get_candidate(candidate_id: str) -> dict[str, Any]:
        for row in store.candidates:
            if str(row.get("candidate_id", "")) == candidate_id:
                return row
        raise HTTPException(status_code=404, detail=f"candidate not found: {candidate_id}")

    @app.get("/search")
    def search(
        gene: Optional[str] = Query(default=None),
        state_class: Optional[str] = Query(default=None),
    ) -> dict[str, Any]:
        registry_hits = store.registry
        candidate_hits = store.candidates

        if state_class:
            registry_hits = [row for row in registry_hits if str(row.get("state_class", "")) == state_class]

        if gene:
            normalized = gene.lower()
            candidate_hits = [row for row in candidate_hits if str(row.get("nearest_gene", "")).lower() == normalized]

        return {
            "registry": registry_hits[:200],
            "candidates": candidate_hits[:200],
        }

    @app.get("/top_hits")
    def top_hits(
        context: str = Query(default="immune_hematopoietic"),
        k: int = Query(default=100, ge=1, le=1000),
    ) -> dict[str, Any]:
        hits = [row for row in store.registry if str(row.get("context_group", "")) == context]
        hits = sorted(hits, key=lambda row: _to_float(row.get("ranking_score", 0.0)), reverse=True)[: int(k)]
        return {
            "context": context,
            "k": int(k),
            "rows": hits,
        }

    @app.get("/downloads")
    def downloads() -> dict[str, Any]:
        return {
            "registry_dir": str(base_dir.resolve()),
            "files": store.downloads(),
        }

    return app


app = create_app()
