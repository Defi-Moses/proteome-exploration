from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from panccre.ingest import ingest_ccre_ref
from panccre.projection import project_fixture_haplotypes

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"


class ProjectionFixtureTests(unittest.TestCase):
    def test_fixture_projection_outputs_rows_and_qc(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            ccre_ref_path = tmp_path / "ccre_ref.jsonl"
            ingest_ccre_ref(
                bed_path=FIXTURE_BED,
                output_path=ccre_ref_path,
                context_group="immune_hematopoietic",
                source_release="fixture-2026-03",
                output_format="jsonl",
            )

            projection_path = tmp_path / "hap_projection.jsonl"
            qc_path = tmp_path / "hap_projection_qc.json"
            result = project_fixture_haplotypes(
                ccre_ref_path=ccre_ref_path,
                haplotypes_path=FIXTURE_HAPLOTYPES,
                output_path=projection_path,
                qc_summary_path=qc_path,
                output_format="jsonl",
                ccre_ref_format="jsonl",
            )

            self.assertEqual(result.row_count, 300)
            self.assertTrue(projection_path.exists())
            self.assertTrue(qc_path.exists())

            rows = projection_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(rows), 300)

            qc = json.loads(qc_path.read_text(encoding="utf-8"))
            self.assertEqual(sum(qc["map_status_counts"].values()), 300)
            self.assertEqual(qc["unique_haplotype_ids"], 3)


if __name__ == "__main__":
    unittest.main()
