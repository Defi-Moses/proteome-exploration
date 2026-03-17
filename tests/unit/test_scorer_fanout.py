from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import pandas as pd

from panccre.candidate_discovery import run_candidate_discovery
from panccre.features import run_feature_build
from panccre.ingest import ingest_ccre_ref
from panccre.projection import project_fixture_haplotypes
from panccre.scorers import run_disagreement_build, run_scorer_fanout, run_shortlist_build
from panccre.state_calling import call_states_from_projection

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"


class ScorerFanoutTests(unittest.TestCase):
    def test_shortlist_fanout_and_disagreement(self) -> None:
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
            run_candidate_discovery(
                ccre_state_path=ccre_state,
                ccre_state_format="jsonl",
                output_path=candidates,
                qc_summary_path=tmp / "replacement_candidates_qc.json",
                output_format="jsonl",
            )
            feature_matrix = tmp / "feature_matrix.jsonl"
            run_feature_build(
                ccre_state_path=ccre_state,
                ccre_state_format="jsonl",
                replacement_candidate_path=candidates,
                replacement_candidate_format="jsonl",
                output_path=feature_matrix,
                output_format="jsonl",
            )

            shortlist = tmp / "shortlist.jsonl"
            shortlist_result = run_shortlist_build(
                feature_matrix_path=feature_matrix,
                feature_matrix_format="jsonl",
                output_path=shortlist,
                output_format="jsonl",
                top_n=50,
            )
            self.assertEqual(shortlist_result.row_count, 50)

            scorer_output = tmp / "scorer_outputs.jsonl"
            fanout_result = run_scorer_fanout(
                feature_matrix_path=feature_matrix,
                feature_matrix_format="jsonl",
                shortlist_path=shortlist,
                shortlist_format="jsonl",
                output_path=scorer_output,
                output_format="jsonl",
                context_group="immune_hematopoietic",
                max_alphagenome_calls=50,
            )
            self.assertGreater(fanout_result.row_count, 0)
            self.assertEqual(fanout_result.alphagenome_calls, 50)

            disagreement = tmp / "disagreement_features.jsonl"
            result_dis = run_disagreement_build(
                scorer_output_path=scorer_output,
                scorer_output_format="jsonl",
                output_path=disagreement,
                output_format="jsonl",
            )
            self.assertGreater(result_dis.row_count, 0)

            score_frame = pd.read_json(scorer_output, lines=True)
            dis_frame = pd.read_json(disagreement, lines=True)
            self.assertEqual(dis_frame.shape[0], score_frame["entity_id"].nunique())

    def test_alphagenome_budget_enforced(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)

            feature_matrix = tmp / "feature_matrix.jsonl"
            frame = pd.DataFrame(
                [
                    {"entity_id": f"E{i}", "entity_type": "ref_state", "feature_name": "coverage_frac", "feature_value": 0.8, "feature_version": "v1"}
                    for i in range(20)
                ]
            )
            frame.to_json(feature_matrix, orient="records", lines=True)

            shortlist = tmp / "shortlist.jsonl"
            run_shortlist_build(
                feature_matrix_path=feature_matrix,
                feature_matrix_format="jsonl",
                output_path=shortlist,
                output_format="jsonl",
                top_n=10,
            )

            with self.assertRaises(ValueError):
                run_scorer_fanout(
                    feature_matrix_path=feature_matrix,
                    feature_matrix_format="jsonl",
                    shortlist_path=shortlist,
                    shortlist_format="jsonl",
                    output_path=tmp / "scorer_outputs.jsonl",
                    output_format="jsonl",
                    context_group="immune_hematopoietic",
                    max_alphagenome_calls=5,
                )


if __name__ == "__main__":
    unittest.main()
