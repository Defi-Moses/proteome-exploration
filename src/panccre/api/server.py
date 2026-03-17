"""FastAPI service for querying registry artifacts."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional, Union

import pandas as pd
from fastapi import FastAPI, HTTPException, Query


def _parquet_available() -> bool:
    try:
        pd.io.parquet.get_engine("auto")
        return True
    except Exception:
        return False


def _read_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        if not _parquet_available():
            raise RuntimeError("Parquet support unavailable")
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".jsonl", ".ndjson"}:
        return pd.read_json(path, lines=True)
    raise ValueError(f"Unsupported file extension: {path}")


def _discover_artifact(base_dir: Path, stem: str) -> Path:
    for ext in ["parquet", "jsonl", "csv"]:
        candidate = base_dir / f"{stem}.{ext}"
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Artifact not found for stem={stem} in {base_dir}")


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records"))


class RegistryStore:
    def __init__(self, registry_dir: Path) -> None:
        self.registry_dir = registry_dir
        self._cache: dict[str, pd.DataFrame] = {}

    def _load(self, key: str, stem: str) -> pd.DataFrame:
        if key in self._cache:
            return self._cache[key]

        path = _discover_artifact(self.registry_dir, stem)
        frame = _read_table(path)
        self._cache[key] = frame
        return frame

    @property
    def registry(self) -> pd.DataFrame:
        return self._load("registry", "polymorphic_ccre_registry")

    @property
    def candidates(self) -> pd.DataFrame:
        return self._load("candidates", "replacement_candidates")

    @property
    def scorers(self) -> pd.DataFrame:
        return self._load("scorers", "scorer_outputs")

    @property
    def validations(self) -> pd.DataFrame:
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
    store = RegistryStore(base_dir)

    app = FastAPI(title="pan-ccre-registry", version="0.1.0")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "registry_dir": str(base_dir.resolve()),
            "counts": {
                "registry": int(store.registry.shape[0]),
                "candidates": int(store.candidates.shape[0]),
                "scorer_outputs": int(store.scorers.shape[0]),
                "validation_links": int(store.validations.shape[0]),
            },
        }

    @app.get("/ccre/{ccre_id}")
    def get_ccre(ccre_id: str) -> dict[str, Any]:
        frame = store.registry
        hits = frame[frame["source_anchor_ccre"] == ccre_id]
        if hits.empty:
            raise HTTPException(status_code=404, detail=f"cCRE not found: {ccre_id}")
        return {
            "ccre_id": ccre_id,
            "rows": _records(hits.sort_values("ranking_score", ascending=False)),
        }

    @app.get("/candidate/{candidate_id}")
    def get_candidate(candidate_id: str) -> dict[str, Any]:
        frame = store.candidates
        hits = frame[frame["candidate_id"] == candidate_id]
        if hits.empty:
            raise HTTPException(status_code=404, detail=f"candidate not found: {candidate_id}")
        return _records(hits)[0]

    @app.get("/search")
    def search(
        gene: Optional[str] = Query(default=None),
        state_class: Optional[str] = Query(default=None),
    ) -> dict[str, Any]:
        registry = store.registry
        candidates = store.candidates

        registry_hits = registry
        candidate_hits = candidates

        if state_class:
            registry_hits = registry_hits[registry_hits["state_class"] == state_class]

        if gene:
            normalized = gene.lower()
            candidate_hits = candidate_hits[candidate_hits["nearest_gene"].astype(str).str.lower() == normalized]

        return {
            "registry": _records(registry_hits.head(200)),
            "candidates": _records(candidate_hits.head(200)),
        }

    @app.get("/top_hits")
    def top_hits(
        context: str = Query(default="immune_hematopoietic"),
        k: int = Query(default=100, ge=1, le=1000),
    ) -> dict[str, Any]:
        frame = store.registry
        hits = frame[frame["context_group"] == context].sort_values("ranking_score", ascending=False).head(k)
        return {
            "context": context,
            "k": int(k),
            "rows": _records(hits),
        }

    @app.get("/downloads")
    def downloads() -> dict[str, Any]:
        return {
            "registry_dir": str(base_dir.resolve()),
            "files": store.downloads(),
        }

    return app


app = create_app()
