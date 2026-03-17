from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from panccre.candidate_discovery import run_candidate_discovery
from panccre.evaluation import run_holdout_build, run_validation_link_build
from panccre.features import run_feature_build
from panccre.ingest import ingest_ccre_ref
from panccre.projection import project_fixture_haplotypes
from panccre.ranking import run_ranking_evaluation
from panccre.state_calling import call_states_from_projection

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"
ASSAY_FIXTURE = Path(__file__).resolve().parents[1] / "golden" / "assays" / "validation_assay_fixture.tsv"


class ValidationRankingTests(unittest.TestCase):
    def test_holdouts_and_ranking_metrics(self) -> None:
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
                feature_version="v1",
            )

            validation_link = tmp / "validation_link.jsonl"
            run_validation_link_build(
                ccre_state_path=ccre_state,
                ccre_state_format="jsonl",
                assay_source_path=ASSAY_FIXTURE,
                assay_source_format="csv",
                output_path=validation_link,
                output_format="jsonl",
            )

            pub_link = tmp / "validation_link_publication.jsonl"
            loc_link = tmp / "validation_link_locus.jsonl"
            run_holdout_build(
                validation_link_path=validation_link,
                validation_link_format="jsonl",
                publication_output_path=pub_link,
                locus_output_path=loc_link,
                output_format="jsonl",
            )

            pub_report = tmp / "ranking_publication_report.json"
            pub_scores = tmp / "ranking_publication_scores.jsonl"
            run_ranking_evaluation(
                feature_matrix_path=feature_matrix,
                feature_matrix_format="jsonl",
                validation_link_path=pub_link,
                validation_link_format="jsonl",
                report_output_path=pub_report,
                scores_output_path=pub_scores,
            )

            report = json.loads(pub_report.read_text(encoding="utf-8"))
            self.assertIn("top_k", report)
            self.assertIn("pr_auc", report)
            self.assertIn("cheap_linear", report["pr_auc"])


if __name__ == "__main__":
    unittest.main()
