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
from panccre.scorers import (
    run_disagreement_ablation,
    run_disagreement_build,
    run_scorer_fanout,
    run_shortlist_build,
)
from panccre.state_calling import call_states_from_projection

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"
ASSAY_FIXTURE = Path(__file__).resolve().parents[1] / "golden" / "assays" / "validation_assay_fixture.tsv"


class DisagreementAblationTests(unittest.TestCase):
    def test_ablation_report(self) -> None:
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
            features = tmp / "feature_matrix.jsonl"
            run_feature_build(
                ccre_state_path=ccre_state,
                ccre_state_format="jsonl",
                replacement_candidate_path=candidates,
                replacement_candidate_format="jsonl",
                output_path=features,
                output_format="jsonl",
            )
            validation = tmp / "validation_link.jsonl"
            run_validation_link_build(
                ccre_state_path=ccre_state,
                ccre_state_format="jsonl",
                assay_source_path=ASSAY_FIXTURE,
                assay_source_format="csv",
                output_path=validation,
                output_format="jsonl",
            )
            pub = tmp / "validation_publication.jsonl"
            loc = tmp / "validation_locus.jsonl"
            run_holdout_build(
                validation_link_path=validation,
                validation_link_format="jsonl",
                publication_output_path=pub,
                locus_output_path=loc,
                output_format="jsonl",
            )

            shortlist = tmp / "shortlist.jsonl"
            run_shortlist_build(
                feature_matrix_path=features,
                feature_matrix_format="jsonl",
                output_path=shortlist,
                output_format="jsonl",
                top_n=60,
            )
            scorer_output = tmp / "scorer_outputs.jsonl"
            run_scorer_fanout(
                feature_matrix_path=features,
                feature_matrix_format="jsonl",
                shortlist_path=shortlist,
                shortlist_format="jsonl",
                output_path=scorer_output,
                output_format="jsonl",
                context_group="immune_hematopoietic",
                max_alphagenome_calls=60,
            )
            disagreement = tmp / "disagreement.jsonl"
            run_disagreement_build(
                scorer_output_path=scorer_output,
                scorer_output_format="jsonl",
                output_path=disagreement,
                output_format="jsonl",
            )

            report = tmp / "ablation.json"
            run_disagreement_ablation(
                feature_matrix_path=features,
                feature_matrix_format="jsonl",
                validation_link_path=pub,
                validation_link_format="jsonl",
                disagreement_path=disagreement,
                disagreement_format="jsonl",
                report_output_path=report,
            )

            payload = json.loads(report.read_text(encoding="utf-8"))
            self.assertIn("base", payload)
            self.assertIn("with_disagreement", payload)
            self.assertIn("lift", payload)


if __name__ == "__main__":
    unittest.main()
