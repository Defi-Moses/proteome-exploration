from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from panccre.ingest import ingest_ccre_ref
from panccre.projection import project_vcf_haplotypes

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"
FIXTURE_VARIANTS = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "hap_projection_variants_fixture.vcf"


class ProjectionVCFTests(unittest.TestCase):
    def test_vcf_projection_outputs_rows_and_qc(self) -> None:
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
            result = project_vcf_haplotypes(
                ccre_ref_path=ccre_ref_path,
                variants_path=FIXTURE_VARIANTS,
                haplotypes_path=FIXTURE_HAPLOTYPES,
                output_path=projection_path,
                qc_summary_path=qc_path,
                output_format="jsonl",
                ccre_ref_format="jsonl",
            )

            self.assertEqual(result.row_count, 300)
            self.assertTrue(projection_path.exists())
            self.assertTrue(qc_path.exists())

            frame = pd.read_json(projection_path, lines=True)
            self.assertEqual(int(frame.shape[0]), 300)
            self.assertEqual(set(frame["mapping_method"].unique()), {"vcf_projection_v1"})

            status_counts = frame["map_status"].value_counts().to_dict()
            self.assertEqual(int(status_counts.get("absent", 0)), 1)
            self.assertEqual(int(status_counts.get("duplicated", 0)), 1)
            self.assertEqual(int(status_counts.get("fractured", 0)), 2)
            self.assertEqual(int(status_counts.get("diverged", 0)), 2)
            self.assertEqual(int(status_counts.get("exact", 0)), 294)

            qc = json.loads(qc_path.read_text(encoding="utf-8"))
            self.assertEqual(sum(qc["map_status_counts"].values()), 300)
            self.assertEqual(qc["unique_haplotype_ids"], 3)

    def test_vcf_projection_respects_max_variants(self) -> None:
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
            result = project_vcf_haplotypes(
                ccre_ref_path=ccre_ref_path,
                variants_path=FIXTURE_VARIANTS,
                haplotypes_path=FIXTURE_HAPLOTYPES,
                output_path=projection_path,
                qc_summary_path=tmp_path / "hap_projection_qc.json",
                output_format="jsonl",
                ccre_ref_format="jsonl",
                max_variants=1,
            )

            self.assertEqual(result.row_count, 300)
            frame = pd.read_json(projection_path, lines=True)
            status_counts = frame["map_status"].value_counts().to_dict()
            self.assertEqual(int(status_counts.get("absent", 0)), 1)
            self.assertEqual(int(status_counts.get("exact", 0)), 299)
            self.assertEqual(int(status_counts.get("duplicated", 0)), 0)


if __name__ == "__main__":
    unittest.main()
