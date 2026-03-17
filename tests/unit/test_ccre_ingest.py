from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from panccre.ingest.ccre import ingest_ccre_ref, parse_ccre_bed

FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"


class CCREIngestTests(unittest.TestCase):
    def test_parse_chr20_fixture_rows(self) -> None:
        rows = parse_ccre_bed(
            FIXTURE_BED,
            context_group="immune_hematopoietic",
            source_release="fixture-2026-03",
        )

        self.assertEqual(len(rows), 100)
        self.assertEqual(rows[0].ccre_id, "EH38E000001")
        self.assertEqual(rows[-1].ccre_id, "EH38E000100")
        self.assertTrue(all(row.chr == "chr20" for row in rows))

    def test_ingest_ccre_ref_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "ccre_ref.jsonl"
            result = ingest_ccre_ref(
                bed_path=FIXTURE_BED,
                output_path=output_path,
                context_group="immune_hematopoietic",
                source_release="fixture-2026-03",
                output_format="jsonl",
            )

            self.assertEqual(result.row_count, 100)
            self.assertTrue(output_path.exists())

            lines = output_path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(lines), 100)

            first = json.loads(lines[0])
            self.assertEqual(first["context_group"], "immune_hematopoietic")

    def test_ingest_ccre_ref_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "ccre_ref.csv"
            result = ingest_ccre_ref(
                bed_path=FIXTURE_BED,
                output_path=output_path,
                context_group="immune_hematopoietic",
                source_release="fixture-2026-03",
                output_format="csv",
            )

            self.assertEqual(result.row_count, 100)
            frame = pd.read_csv(output_path)
            self.assertEqual(frame.shape[0], 100)
            self.assertIn("ccre_id", frame.columns)

    def test_ingest_ccre_ref_parquet_behaviour(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "ccre_ref.parquet"
            try:
                result = ingest_ccre_ref(
                    bed_path=FIXTURE_BED,
                    output_path=output_path,
                    context_group="immune_hematopoietic",
                    source_release="fixture-2026-03",
                    output_format="parquet",
                )
                self.assertEqual(result.row_count, 100)
                self.assertTrue(output_path.exists())
            except RuntimeError as exc:
                self.assertIn("Parquet output requires", str(exc))


if __name__ == "__main__":
    unittest.main()
