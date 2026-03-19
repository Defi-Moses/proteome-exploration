from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from panccre.cli.main import main

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"
ASSAY_FIXTURE = Path(__file__).resolve().parents[1] / "golden" / "assays" / "validation_assay_fixture.tsv"


class ReportBundleCLITests(unittest.TestCase):
    def test_build_phase1_report_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            smoke_dir = tmp / "smoke"
            projection_dir = tmp / "projection"
            state_dir = tmp / "state"
            candidate_dir = tmp / "candidates"
            feature_dir = tmp / "features"
            validation_dir = tmp / "validation"
            scorer_dir = tmp / "scorers"
            ranking_dir = tmp / "ranking"
            registry_dir = tmp / "registry"
            report_dir = tmp / "reports"

            self.assertEqual(main(["smoke-ingest", "--fixture-bed", str(FIXTURE_BED), "--output-dir", str(smoke_dir), "--output-format", "jsonl", "--source-release", "fixture-2026-03"]), 0)
            self.assertEqual(main(["project-fixture", "--ccre-ref", str(smoke_dir / "ccre_ref.jsonl"), "--ccre-ref-format", "jsonl", "--haplotypes", str(FIXTURE_HAPLOTYPES), "--output-dir", str(projection_dir), "--output-format", "jsonl"]), 0)
            self.assertEqual(main(["call-states", "--hap-projection", str(projection_dir / "hap_projection.jsonl"), "--hap-projection-format", "jsonl", "--output-dir", str(state_dir), "--output-format", "jsonl"]), 0)
            self.assertEqual(main(["discover-candidates", "--ccre-state", str(state_dir / "ccre_state.jsonl"), "--ccre-state-format", "jsonl", "--output-dir", str(candidate_dir), "--output-format", "jsonl"]), 0)
            self.assertEqual(main(["featurize", "--ccre-state", str(state_dir / "ccre_state.jsonl"), "--ccre-state-format", "jsonl", "--replacement-candidates", str(candidate_dir / "replacement_candidates.jsonl"), "--replacement-candidates-format", "jsonl", "--output-dir", str(feature_dir), "--output-format", "jsonl"]), 0)
            self.assertEqual(main(["build-validation-link", "--ccre-state", str(state_dir / "ccre_state.jsonl"), "--ccre-state-format", "jsonl", "--assay-source", str(ASSAY_FIXTURE), "--assay-source-format", "csv", "--output-dir", str(validation_dir), "--output-format", "jsonl"]), 0)
            self.assertEqual(main(["build-holdouts", "--validation-link", str(validation_dir / "validation_link.jsonl"), "--validation-link-format", "jsonl", "--output-dir", str(validation_dir), "--output-format", "jsonl"]), 0)
            self.assertEqual(main(["evaluate-ranking", "--feature-matrix", str(feature_dir / "feature_matrix.jsonl"), "--feature-matrix-format", "jsonl", "--publication-validation", str(validation_dir / "validation_link_publication.jsonl"), "--publication-validation-format", "jsonl", "--locus-validation", str(validation_dir / "validation_link_locus.jsonl"), "--locus-validation-format", "jsonl", "--output-dir", str(ranking_dir)]), 0)
            self.assertEqual(main(["shortlist", "--feature-matrix", str(feature_dir / "feature_matrix.jsonl"), "--feature-matrix-format", "jsonl", "--top", "80", "--output-dir", str(scorer_dir), "--output-format", "jsonl"]), 0)
            self.assertEqual(main(["score-fanout", "--feature-matrix", str(feature_dir / "feature_matrix.jsonl"), "--feature-matrix-format", "jsonl", "--shortlist", str(scorer_dir / "shortlist.jsonl"), "--shortlist-format", "jsonl", "--max-alphagenome-calls", "80", "--output-dir", str(scorer_dir), "--output-format", "jsonl"]), 0)
            self.assertEqual(main(["compute-disagreement", "--scorer-outputs", str(scorer_dir / "scorer_outputs.jsonl"), "--scorer-outputs-format", "jsonl", "--output-dir", str(scorer_dir), "--output-format", "jsonl"]), 0)
            self.assertEqual(main(["run-ablations", "--feature-matrix", str(feature_dir / "feature_matrix.jsonl"), "--feature-matrix-format", "jsonl", "--disagreement-features", str(scorer_dir / "disagreement_features.jsonl"), "--disagreement-features-format", "jsonl", "--publication-validation", str(validation_dir / "validation_link_publication.jsonl"), "--publication-validation-format", "jsonl", "--locus-validation", str(validation_dir / "validation_link_locus.jsonl"), "--locus-validation-format", "jsonl", "--output-dir", str(ranking_dir)]), 0)
            self.assertEqual(main(["build-registry", "--ccre-state", str(state_dir / "ccre_state.jsonl"), "--ccre-state-format", "jsonl", "--replacement-candidates", str(candidate_dir / "replacement_candidates.jsonl"), "--replacement-candidates-format", "jsonl", "--scorer-outputs", str(scorer_dir / "scorer_outputs.jsonl"), "--scorer-outputs-format", "jsonl", "--validation-links", str(validation_dir / "validation_link.jsonl"), "--validation-links-format", "jsonl", "--output-dir", str(registry_dir), "--output-format", "jsonl"]), 0)

            self.assertEqual(
                main(
                    [
                        "build-phase1-report",
                        "--registry-dir",
                        str(registry_dir),
                        "--publication-ranking-report",
                        str(ranking_dir / "ranking_publication_report.json"),
                        "--locus-ranking-report",
                        str(ranking_dir / "ranking_locus_report.json"),
                        "--disagreement-features",
                        str(scorer_dir / "disagreement_features.jsonl"),
                        "--ablation-summary",
                        str(ranking_dir / "disagreement_ablation_summary.json"),
                        "--output-dir",
                        str(report_dir),
                        "--top-hits-k",
                        "100",
                        "--case-study-count",
                        "3",
                    ]
                ),
                0,
            )

            self.assertTrue((report_dir / "report.md").exists())
            self.assertTrue((report_dir / "tables" / "top_100_ranked_loci.csv").exists())
            self.assertTrue((report_dir / "figures" / "state_class_distribution.csv").exists())
            self.assertTrue((report_dir / "bundle_manifest.json").exists())

            bundle = json.loads((report_dir / "bundle_manifest.json").read_text(encoding="utf-8"))
            self.assertIn("files", bundle)
            case_files = list((report_dir / "case_studies").glob("*.json"))
            self.assertGreaterEqual(len(case_files), 3)


if __name__ == "__main__":
    unittest.main()
