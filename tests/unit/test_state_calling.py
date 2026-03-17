from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from panccre.ingest import ingest_ccre_ref
from panccre.projection import project_fixture_haplotypes
from panccre.state_calling import call_states_from_projection

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"
FIXTURE_HAPLOTYPES = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "haplotypes_chr20_fixture.tsv"


class StateCallingTests(unittest.TestCase):
    def test_call_states_from_projection_fixture(self) -> None:
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
            projection_qc = tmp / "hap_projection_qc.json"
            project_fixture_haplotypes(
                ccre_ref_path=ccre_ref,
                ccre_ref_format="jsonl",
                haplotypes_path=FIXTURE_HAPLOTYPES,
                output_path=hap_projection,
                qc_summary_path=projection_qc,
                output_format="jsonl",
            )

            ccre_state = tmp / "ccre_state.jsonl"
            state_qc = tmp / "ccre_state_qc.json"
            result = call_states_from_projection(
                projection_path=hap_projection,
                projection_format="jsonl",
                output_path=ccre_state,
                qc_summary_path=state_qc,
                output_format="jsonl",
            )

            self.assertEqual(result.row_count, 300)
            self.assertTrue(ccre_state.exists())
            self.assertTrue(state_qc.exists())

            rows = ccre_state.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(rows), 300)

            qc = json.loads(state_qc.read_text(encoding="utf-8"))
            self.assertEqual(sum(qc["state_class_counts"].values()), 300)
            self.assertIn("absent", qc["state_class_counts"])
            self.assertIn("fractured", qc["state_class_counts"])


if __name__ == "__main__":
    unittest.main()
