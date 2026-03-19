from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

import panccre.normalize.engreitz as eng


class NormalizeEngreitzTests(unittest.TestCase):
    def test_derives_label_with_regulated_priority(self) -> None:
        self.assertEqual(eng._derive_label("TRUE", "FALSE", 1.0), "hit")
        self.assertEqual(eng._derive_label("FALSE", "TRUE", -1.0), "non-hit")
        self.assertEqual(eng._derive_label(None, "TRUE", -0.2), "hit")
        self.assertEqual(eng._derive_label(None, "TRUE", 0.2), "non-hit")
        self.assertIsNone(eng._derive_label(None, None, -0.2))

    def test_derives_study_id_and_year(self) -> None:
        self.assertEqual(eng._derive_study_id("Dataset Alpha 2024", None), "Dataset_Alpha_2024")
        self.assertEqual(eng._derive_study_id("", "Gasperini et al. 2019"), "Gasperini_2019")
        self.assertIsNone(eng._derive_study_id("", "no_year_here"))
        self.assertEqual(eng._parse_publication_year("Dataset 2024", ""), 2024)
        self.assertEqual(eng._parse_publication_year("", "Nasser 2021"), 2021)
        self.assertIsNone(eng._parse_publication_year("", "no_year"))

    def test_normalization_outputs_contract_frame(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            source_path = tmp / "source.tsv.gz"
            hap_path = tmp / "haplotypes.tsv"
            ccre_path = tmp / "ccre.bed"
            output_path = tmp / "assay.csv"

            source = pd.DataFrame(
                [
                    {
                        "chrom": "chr20",
                        "chromStart": 100,
                        "chromEnd": 200,
                        "EffectSize": -0.6,
                        "CellType": "K562",
                        "Dataset": "STUDY_ALPHA_2024",
                        "Reference": "Nasser 2021",
                        "Regulated": "TRUE",
                        "Significant": "TRUE",
                    },
                    {
                        "chrom": "20",
                        "chromStart": 300,
                        "chromEnd": 360,
                        "EffectSize": 0.2,
                        "CellType": "K562",
                        "Dataset": "STUDY_BETA_2025",
                        "Reference": "Gasperini 2019",
                        "Regulated": "FALSE",
                        "Significant": "TRUE",
                    },
                    {
                        "chrom": "chr20",
                        "chromStart": 900,
                        "chromEnd": 950,
                        "EffectSize": "nan",
                        "CellType": "GM12878",
                        "Dataset": "",
                        "Reference": "",
                        "Regulated": "",
                        "Significant": "",
                    },
                ]
            )
            source.to_csv(source_path, sep="\t", index=False, compression="gzip")

            hap_path.write_text("haplotype_id\nHG00438\nHG00621\n", encoding="utf-8")
            ccre_path.write_text("chr20\t0\t10\tEH38E000001\n", encoding="utf-8")

            mocked_mapping = pd.DataFrame(
                [
                    {"src_row": 0, "ccre_id": "EH38E000101", "overlap_bp": 80, "mid_dist": 5},
                    {"src_row": 1, "ccre_id": "EH38E000102", "overlap_bp": 60, "mid_dist": 7},
                ]
            )
            with patch.object(eng, "_map_engreitz_rows_to_ccre", return_value=mocked_mapping):
                result = eng.normalize_engreitz_assay_source(
                    source_path=source_path,
                    ccre_bed_path=ccre_path,
                    haplotypes_path=hap_path,
                    output_path=output_path,
                )

            out = pd.read_csv(result.output_path)
            self.assertEqual(
                list(out.columns),
                [
                    "ccre_id",
                    "haplotype_id",
                    "study_id",
                    "assay_type",
                    "label",
                    "effect_size",
                    "cell_context",
                    "publication_year",
                ],
            )
            self.assertEqual(int(out.shape[0]), 4)
            self.assertEqual(set(out["label"]), {"hit", "non-hit"})
            self.assertEqual(set(out["haplotype_id"]), {"HG00438", "HG00621"})
            self.assertEqual(int(out["study_id"].nunique()), 2)

            rejects = pd.read_csv(result.rejects_path)
            self.assertEqual(int(rejects.shape[0]), 1)
            self.assertIn("invalid_effect_size", str(rejects.iloc[0]["reject_reason"]))

            summary = json.loads(result.summary_path.read_text(encoding="utf-8"))
            self.assertEqual(int(summary["source_row_count"]), 3)
            self.assertEqual(int(summary["output_row_count"]), 4)
            self.assertEqual(int(summary["study_count"]), 2)


if __name__ == "__main__":
    unittest.main()
