from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

import pandas as pd

from panccre.ingest.ccre import (
    CCRE_REF_COLUMNS,
    ingest_ccre_ref,
    parse_ccre_bed,
    read_ccre_ref,
    validate_ccre_ref_frame,
)

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

            frame = read_ccre_ref(output_path, input_format="jsonl")
            self.assertEqual(frame.shape[0], 100)
            self.assertEqual(list(frame.columns), CCRE_REF_COLUMNS)

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

    def test_parse_rejects_duplicate_ccre_id(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bed = Path(tmpdir) / "dup.bed"
            bed.write_text(
                "chr20\t100\t120\tEH38E000001\t0\t+\tpELS\t3\n"
                "chr20\t200\t220\tEH38E000001\t0\t+\tpELS\t4\n",
                encoding="utf-8",
            )
            with self.assertRaises(ValueError):
                parse_ccre_bed(bed, context_group="immune_hematopoietic", source_release="fixture")

    def test_parse_rejects_invalid_strand(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bed = Path(tmpdir) / "bad_strand.bed"
            bed.write_text("chr20\t100\t120\tEH38E000001\t0\t*\tpELS\t3\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                parse_ccre_bed(bed, context_group="immune_hematopoietic", source_release="fixture")

    def test_parse_supports_encode_screen_v4_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bed = Path(tmpdir) / "screen_v4.bed"
            bed.write_text(
                "chr1\t10033\t10250\tEH38D4327497\tEH38E2776516\tpELS\n"
                "chr1\t10385\t10713\tEH38D4327498\tEH38E2776517\tdELS\t12\n",
                encoding="utf-8",
            )
            rows = parse_ccre_bed(
                bed,
                context_group="immune_hematopoietic",
                source_release="encode-v4-2026-01",
            )

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0].ccre_id, "EH38E2776516")
            self.assertEqual(rows[0].strand, ".")
            self.assertEqual(rows[0].ccre_class, "pELS")
            self.assertEqual(rows[0].biosample_count, 0)
            self.assertEqual(rows[1].ccre_id, "EH38E2776517")
            self.assertEqual(rows[1].ccre_class, "dELS")
            self.assertEqual(rows[1].biosample_count, 12)

    def test_validate_ccre_ref_frame_rejects_contract_drift(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "ccre_id": "EH38E000001",
                    "chr": "chr20",
                    "start": 100,
                    "end": 120,
                    "strand": "+",
                    "ccre_class": "pELS",
                    "biosample_count": 1,
                    "context_group": "immune_hematopoietic",
                    "anchor_width": 20,
                    "source_release": "fixture",
                }
            ]
        )
        frame = frame.drop(columns=["source_release"])
        with self.assertRaises(ValueError):
            validate_ccre_ref_frame(frame)


if __name__ == "__main__":
    unittest.main()
