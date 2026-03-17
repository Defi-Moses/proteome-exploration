from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from fastapi.testclient import TestClient

from panccre.api import create_app
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


if __name__ == "__main__":
    unittest.main()
