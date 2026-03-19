from __future__ import annotations

import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from panccre.api import create_app
from panccre.api.server import (
    REGISTRY_COLUMNS,
    REPLACEMENT_CANDIDATE_COLUMNS,
    SCORER_OUTPUT_COLUMNS,
    VALIDATION_LINK_COLUMNS,
)
from panccre.candidate_discovery import run_candidate_discovery
from panccre.evaluation import run_validation_link_build
from panccre.features import run_feature_build
from panccre.ingest import ingest_ccre_ref
from panccre.projection import project_fixture_haplotypes
from panccre.registry import run_registry_build
from panccre.scorers import run_scorer_fanout, run_shortlist_build
from panccre.state_calling import call_states_from_projection

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"
ASSAY_FIXTURE = Path(__file__).resolve().parents[1] / "golden" / "assays" / "validation_assay_fixture.tsv"


class APIServerTests(unittest.TestCase):
    def test_api_endpoints(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)

            ccre_ref = tmp / "ccre_ref.jsonl"
            ingest_ccre_ref(
                bed_path=FIXTURE_BED,
                output_path=ccre_ref,
                context_group="immune_hematopoietic",
                source_release="fixture-2026-03",
                output_format="jsonl",
            )
            projection = tmp / "hap_projection.jsonl"
            project_fixture_haplotypes(
                ccre_ref_path=ccre_ref,
                ccre_ref_format="jsonl",
                haplotypes_path=FIXTURE_HAPLOTYPES,
                output_path=projection,
                qc_summary_path=tmp / "projection_qc.json",
                output_format="jsonl",
            )
            state = tmp / "ccre_state.jsonl"
            call_states_from_projection(
                projection_path=projection,
                projection_format="jsonl",
                output_path=state,
                qc_summary_path=tmp / "state_qc.json",
                output_format="jsonl",
            )
            candidates = tmp / "replacement_candidates.jsonl"
            run_candidate_discovery(
                ccre_state_path=state,
                ccre_state_format="jsonl",
                output_path=candidates,
                qc_summary_path=tmp / "candidates_qc.json",
                output_format="jsonl",
            )
            features = tmp / "feature_matrix.jsonl"
            run_feature_build(
                ccre_state_path=state,
                ccre_state_format="jsonl",
                replacement_candidate_path=candidates,
                replacement_candidate_format="jsonl",
                output_path=features,
                output_format="jsonl",
            )
            shortlist = tmp / "shortlist.jsonl"
            run_shortlist_build(
                feature_matrix_path=features,
                feature_matrix_format="jsonl",
                output_path=shortlist,
                output_format="jsonl",
                top_n=30,
            )
            scorer_outputs = tmp / "scorer_outputs.jsonl"
            run_scorer_fanout(
                feature_matrix_path=features,
                feature_matrix_format="jsonl",
                shortlist_path=shortlist,
                shortlist_format="jsonl",
                output_path=scorer_outputs,
                output_format="jsonl",
                context_group="immune_hematopoietic",
                max_alphagenome_calls=30,
            )
            validation = tmp / "validation_link.jsonl"
            run_validation_link_build(
                ccre_state_path=state,
                ccre_state_format="jsonl",
                assay_source_path=ASSAY_FIXTURE,
                assay_source_format="csv",
                output_path=validation,
                output_format="jsonl",
            )
            registry_dir = tmp / "registry"
            run_registry_build(
                ccre_state_path=state,
                ccre_state_format="jsonl",
                replacement_candidates_path=candidates,
                replacement_candidates_format="jsonl",
                scorer_output_path=scorer_outputs,
                scorer_output_format="jsonl",
                validation_link_path=validation,
                validation_link_format="jsonl",
                output_dir=registry_dir,
                output_format="jsonl",
            )

            app = create_app(registry_dir=registry_dir)
            client = TestClient(app)

            health = client.get("/health")
            self.assertEqual(health.status_code, 200)
            self.assertEqual(health.json()["status"], "ok")

            ccre = client.get("/ccre/EH38E000001")
            self.assertEqual(ccre.status_code, 200)
            self.assertGreater(len(ccre.json()["rows"]), 0)

            downloads = client.get("/downloads")
            self.assertEqual(downloads.status_code, 200)
            self.assertIn("files", downloads.json())

            top_hits = client.get("/top_hits", params={"k": 5})
            self.assertEqual(top_hits.status_code, 200)
            self.assertEqual(top_hits.json()["k"], 5)

            candidate_id = json.loads((registry_dir / "replacement_candidates.jsonl").read_text(encoding="utf-8").splitlines()[0])["candidate_id"]
            candidate = client.get(f"/candidate/{candidate_id}")
            self.assertEqual(candidate.status_code, 200)
            self.assertEqual(candidate.json()["candidate_id"], candidate_id)

    def test_internal_registry_sync_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            source_registry = tmp / "source_registry"
            source_registry.mkdir(parents=True, exist_ok=True)

            def _write_csv(path: Path, headers: list[str], row: dict[str, str]) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("w", encoding="utf-8") as handle:
                    handle.write(",".join(headers) + "\n")
                    handle.write(",".join(str(row.get(key, "")) for key in headers) + "\n")

            _write_csv(
                source_registry / "polymorphic_ccre_registry.csv",
                REGISTRY_COLUMNS,
                {
                    "entity_id": "ent_1",
                    "source_anchor_ccre": "EH38E000001",
                    "haplotype_id": "HG00438",
                    "state_class": "conserved",
                    "ref_chr": "chr20",
                    "ref_start": "100500",
                    "ref_end": "100750",
                    "context_group": "immune_hematopoietic",
                    "ranking_score": "0.88",
                    "qc_flag": "ok",
                },
            )
            _write_csv(
                source_registry / "replacement_candidates.csv",
                REPLACEMENT_CANDIDATE_COLUMNS,
                {
                    "candidate_id": "cand_1",
                    "parent_ccre_id": "EH38E000001",
                    "haplotype_id": "HG00438",
                    "window_class": "duplicate_neighbor",
                    "alt_contig": "chr20",
                    "alt_start": "100490",
                    "alt_end": "100760",
                    "seq_len": "270",
                    "repeat_class": "LINE",
                    "te_family": "L1",
                    "motif_count": "3",
                    "gc_content": "0.45",
                    "nearest_gene": "GENE1000",
                    "nearest_gene_distance": "5000",
                },
            )
            _write_csv(
                source_registry / "scorer_outputs.csv",
                SCORER_OUTPUT_COLUMNS,
                {
                    "entity_id": "ent_1",
                    "entity_type": "registry_entry",
                    "scorer_name": "cheap_model_v1",
                    "assay_proxy": "none",
                    "context_group": "immune_hematopoietic",
                    "ref_score": "0.42",
                    "alt_score": "0.51",
                    "delta_score": "0.09",
                    "uncertainty": "0.11",
                    "run_id": "sync-test",
                },
            )
            _write_csv(
                source_registry / "validation_links.csv",
                VALIDATION_LINK_COLUMNS,
                {
                    "entity_id": "ent_1",
                    "entity_type": "registry_entry",
                    "study_id": "study_1",
                    "assay_type": "crispri",
                    "label": "1",
                    "effect_size": "0.2",
                    "cell_context": "K562",
                    "publication_year": "2024",
                    "holdout_group": "publication",
                },
            )

            manifest_payload = {
                "output_format": "csv",
                "row_counts": {
                    "polymorphic_ccre_registry": 1,
                    "replacement_candidates": 1,
                    "scorer_outputs": 1,
                    "validation_links": 1,
                },
                "files": {
                    "polymorphic_ccre_registry": str((source_registry / "polymorphic_ccre_registry.csv").resolve()),
                    "replacement_candidates": str((source_registry / "replacement_candidates.csv").resolve()),
                    "scorer_outputs": str((source_registry / "scorer_outputs.csv").resolve()),
                    "validation_links": str((source_registry / "validation_links.csv").resolve()),
                },
            }
            (source_registry / "registry_manifest.json").write_text(
                json.dumps(manifest_payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

            archive_path = tmp / "registry_payload.tar.gz"
            with tarfile.open(archive_path, mode="w:gz") as handle:
                handle.add(source_registry, arcname="registry")

            target_registry = tmp / "api_registry"
            with patch.dict(
                os.environ,
                {
                    "PANCCRE_AUTO_SEED_REGISTRY": "0",
                    "PANCCRE_REGISTRY_SYNC_TOKEN": "sync-token-test",
                },
                clear=False,
            ):
                app = create_app(registry_dir=target_registry)
                client = TestClient(app)

                sync_response = client.post(
                    "/internal/registry/sync",
                    content=archive_path.read_bytes(),
                    headers={
                        "content-type": "application/gzip",
                        "x-panccre-sync-token": "sync-token-test",
                        "x-panccre-run-tag": "run-sync-test",
                    },
                )
                self.assertEqual(sync_response.status_code, 200)
                self.assertEqual(sync_response.json()["status"], "ok")
                self.assertEqual(sync_response.json()["run_tag"], "run-sync-test")

                health = client.get("/health")
                self.assertEqual(health.status_code, 200)
                self.assertEqual(health.json()["status"], "ok")

                downloads = client.get("/downloads")
                self.assertEqual(downloads.status_code, 200)
                files = downloads.json()["files"]
                self.assertIn("polymorphic_ccre_registry", files)
                self.assertTrue(str(files["polymorphic_ccre_registry"]).startswith(str(target_registry.resolve())))


if __name__ == "__main__":
    unittest.main()
