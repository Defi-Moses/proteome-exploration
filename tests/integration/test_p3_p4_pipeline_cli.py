from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from panccre.cli.main import main

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"
ASSAY_FIXTURE = Path(__file__).resolve().parents[1] / "golden" / "assays" / "validation_assay_fixture.tsv"


class P3P4PipelineCLITests(unittest.TestCase):
    def test_full_p3_p4_cli_chain(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)

            smoke_dir = tmp / "smoke"
            projection_dir = tmp / "projection"
            state_dir = tmp / "state"
            candidate_dir = tmp / "candidates"
            feature_dir = tmp / "features"
            validation_dir = tmp / "validation"
            ranking_dir = tmp / "ranking"

            self.assertEqual(
                main(
                    [
                        "smoke-ingest",
                        "--fixture-bed",
                        str(FIXTURE_BED),
                        "--output-dir",
                        str(smoke_dir),
                        "--output-format",
                        "jsonl",
                        "--source-release",
                        "fixture-2026-03",
                    ]
                ),
                0,
            )

            self.assertEqual(
                main(
                    [
                        "project-fixture",
                        "--ccre-ref",
                        str(smoke_dir / "ccre_ref.jsonl"),
                        "--ccre-ref-format",
                        "jsonl",
                        "--haplotypes",
                        str(FIXTURE_HAPLOTYPES),
                        "--output-dir",
                        str(projection_dir),
                        "--output-format",
                        "jsonl",
                    ]
                ),
                0,
            )

            self.assertEqual(
                main(
                    [
                        "call-states",
                        "--hap-projection",
                        str(projection_dir / "hap_projection.jsonl"),
                        "--hap-projection-format",
                        "jsonl",
                        "--output-dir",
                        str(state_dir),
                        "--output-format",
                        "jsonl",
                    ]
                ),
                0,
            )

            self.assertEqual(
                main(
                    [
                        "discover-candidates",
                        "--ccre-state",
                        str(state_dir / "ccre_state.jsonl"),
                        "--ccre-state-format",
                        "jsonl",
                        "--output-dir",
                        str(candidate_dir),
                        "--output-format",
                        "jsonl",
                    ]
                ),
                0,
            )

            self.assertEqual(
                main(
                    [
                        "featurize",
                        "--ccre-state",
                        str(state_dir / "ccre_state.jsonl"),
                        "--ccre-state-format",
                        "jsonl",
                        "--replacement-candidates",
                        str(candidate_dir / "replacement_candidates.jsonl"),
                        "--replacement-candidates-format",
                        "jsonl",
                        "--output-dir",
                        str(feature_dir),
                        "--output-format",
                        "jsonl",
                    ]
                ),
                0,
            )

            self.assertEqual(
                main(
                    [
                        "build-validation-link",
                        "--ccre-state",
                        str(state_dir / "ccre_state.jsonl"),
                        "--ccre-state-format",
                        "jsonl",
                        "--assay-source",
                        str(ASSAY_FIXTURE),
                        "--assay-source-format",
                        "csv",
                        "--output-dir",
                        str(validation_dir),
                        "--output-format",
                        "jsonl",
                    ]
                ),
                0,
            )

            self.assertEqual(
                main(
                    [
                        "build-holdouts",
                        "--validation-link",
                        str(validation_dir / "validation_link.jsonl"),
                        "--validation-link-format",
                        "jsonl",
                        "--output-dir",
                        str(validation_dir),
                        "--output-format",
                        "jsonl",
                    ]
                ),
                0,
            )

            self.assertEqual(
                main(
                    [
                        "evaluate-ranking",
                        "--feature-matrix",
                        str(feature_dir / "feature_matrix.jsonl"),
                        "--feature-matrix-format",
                        "jsonl",
                        "--publication-validation",
                        str(validation_dir / "validation_link_publication.jsonl"),
                        "--publication-validation-format",
                        "jsonl",
                        "--locus-validation",
                        str(validation_dir / "validation_link_locus.jsonl"),
                        "--locus-validation-format",
                        "jsonl",
                        "--output-dir",
                        str(ranking_dir),
                    ]
                ),
                0,
            )

            comparison = ranking_dir / "baseline_comparison.json"
            self.assertTrue(comparison.exists())

            payload = json.loads(comparison.read_text(encoding="utf-8"))
            self.assertIn("publication", payload)
            self.assertIn("locus", payload)
            self.assertIn("top_k", payload["publication"])


if __name__ == "__main__":
    unittest.main()
