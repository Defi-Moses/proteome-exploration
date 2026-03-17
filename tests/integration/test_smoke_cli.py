from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from panccre.cli.main import main
from panccre.manifests.builder import build_manifest_entry, write_manifest_file

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"


class SmokeCLITests(unittest.TestCase):
    def test_smoke_cli_validates_manifest_and_ingests_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            manifest = build_manifest_entry(
                file_path=FIXTURE_BED,
                source_id="encode_ccre_chr20_fixture",
                name="Chromosome 20 fixture",
                version="2026-03",
                download_url="file://tests/golden/chr20/encode_ccre_chr20_fixture.bed",
                download_date="2026-03-17",
                license_name="public",
                genome_build="GRCh38",
                parser_version="0.1.0",
                file_format="bed",
                notes="synthetic fixture",
            )

            manifest_path = tmp_path / "fixture_manifest.json"
            write_manifest_file(manifest, manifest_path)

            output_dir = tmp_path / "smoke"
            exit_code = main(
                [
                    "smoke-ingest",
                    "--fixture-bed",
                    str(FIXTURE_BED),
                    "--manifest",
                    str(manifest_path),
                    "--output-dir",
                    str(output_dir),
                    "--source-release",
                    "fixture-2026-03",
                    "--context-group",
                    "immune_hematopoietic",
                    "--output-format",
                    "jsonl",
                ]
            )

            self.assertEqual(exit_code, 0)

            ccre_ref_path = output_dir / "ccre_ref.jsonl"
            run_manifest_path = output_dir / "run_manifest.json"

            self.assertTrue(ccre_ref_path.exists())
            self.assertTrue(run_manifest_path.exists())

            run_manifest = json.loads(run_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(run_manifest["row_count"], 100)
            self.assertEqual(run_manifest["params"]["output_format"], "jsonl")


if __name__ == "__main__":
    unittest.main()
