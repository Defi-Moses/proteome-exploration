from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import pandas as pd

from panccre.candidate_discovery import run_candidate_discovery
from panccre.features import run_feature_build
from panccre.ingest import ingest_ccre_ref
from panccre.projection import project_fixture_haplotypes
from panccre.state_calling import call_states_from_projection

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"


class CandidateFeatureTests(unittest.TestCase):
    def test_candidate_discovery_and_feature_matrix(self) -> None:
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

            hap_projection = tmp / "hap_projection.jsonl"
            project_fixture_haplotypes(
                ccre_ref_path=ccre_ref,
                ccre_ref_format="jsonl",
                haplotypes_path=FIXTURE_HAPLOTYPES,
                output_path=hap_projection,
                qc_summary_path=tmp / "hap_projection_qc.json",
                output_format="jsonl",
            )

            ccre_state = tmp / "ccre_state.jsonl"
            call_states_from_projection(
                projection_path=hap_projection,
                projection_format="jsonl",
                output_path=ccre_state,
                qc_summary_path=tmp / "ccre_state_qc.json",
                output_format="jsonl",
            )

            candidates = tmp / "replacement_candidates.jsonl"
            result_candidates = run_candidate_discovery(
                ccre_state_path=ccre_state,
                ccre_state_format="jsonl",
                output_path=candidates,
                qc_summary_path=tmp / "replacement_candidates_qc.json",
                output_format="jsonl",
            )
            self.assertGreater(result_candidates.row_count, 0)

            feature_matrix = tmp / "feature_matrix.jsonl"
            result_features = run_feature_build(
                ccre_state_path=ccre_state,
                ccre_state_format="jsonl",
                replacement_candidate_path=candidates,
                replacement_candidate_format="jsonl",
                output_path=feature_matrix,
                output_format="jsonl",
                feature_version="v1",
            )
            self.assertGreater(result_features.row_count, 0)

            frame = pd.read_json(feature_matrix, lines=True)
            self.assertIn("ref_state", set(frame["entity_type"]))
            self.assertIn("replacement_candidate", set(frame["entity_type"]))


if __name__ == "__main__":
    unittest.main()
