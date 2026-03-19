from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from panccre.cli.main import main

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"
FIXTURE_VARIANTS = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "hap_projection_variants_fixture.vcf"


class ProjectionCLITests(unittest.TestCase):
    def test_project_fixture_cli_after_ingest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            smoke_dir = tmp_path / "smoke"

            ingest_exit = main(
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
            )
            self.assertEqual(ingest_exit, 0)

            projection_dir = tmp_path / "projection"
            project_exit = main(
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
            )
            self.assertEqual(project_exit, 0)

            projection_path = projection_dir / "hap_projection.jsonl"
            qc_path = projection_dir / "hap_projection_qc.json"
            run_manifest_path = projection_dir / "run_manifest.json"

            self.assertTrue(projection_path.exists())
            self.assertTrue(qc_path.exists())
            self.assertTrue(run_manifest_path.exists())

            qc = json.loads(qc_path.read_text(encoding="utf-8"))
            self.assertEqual(sum(qc["map_status_counts"].values()), 300)

    def test_project_vcf_cli_after_ingest(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            smoke_dir = tmp_path / "smoke"

            ingest_exit = main(
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
            )
            self.assertEqual(ingest_exit, 0)

            projection_dir = tmp_path / "projection_vcf"
            project_exit = main(
                [
                    "project-vcf",
                    "--ccre-ref",
                    str(smoke_dir / "ccre_ref.jsonl"),
                    "--ccre-ref-format",
                    "jsonl",
                    "--variants",
                    str(FIXTURE_VARIANTS),
                    "--haplotypes",
                    str(FIXTURE_HAPLOTYPES),
                    "--output-dir",
                    str(projection_dir),
                    "--output-format",
                    "jsonl",
                ]
            )
            self.assertEqual(project_exit, 0)

            projection_path = projection_dir / "hap_projection.jsonl"
            qc_path = projection_dir / "hap_projection_qc.json"
            run_manifest_path = projection_dir / "run_manifest.json"

            self.assertTrue(projection_path.exists())
            self.assertTrue(qc_path.exists())
            self.assertTrue(run_manifest_path.exists())

            qc = json.loads(qc_path.read_text(encoding="utf-8"))
            self.assertEqual(sum(qc["map_status_counts"].values()), 300)
            self.assertGreater(int(qc["map_status_counts"].get("absent", 0)), 0)


if __name__ == "__main__":
    unittest.main()
