from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from panccre.reports import freeze_evaluation


class FreezeEvaluationTests(unittest.TestCase):
    def test_freeze_evaluation_copies_required_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            validation = root / "validation"
            ranking = root / "ranking"
            output_root = root / "processed"
            validation.mkdir(parents=True, exist_ok=True)
            ranking.mkdir(parents=True, exist_ok=True)

            (validation / "validation_link_publication.jsonl").write_text('{"x":1}\n', encoding="utf-8")
            (validation / "validation_link_locus.jsonl").write_text('{"x":2}\n', encoding="utf-8")
            (validation / "holdout_summary.json").write_text('{"ok":true}\n', encoding="utf-8")

            (ranking / "ranking_publication_report.json").write_text('{"top_k":{}}\n', encoding="utf-8")
            (ranking / "ranking_locus_report.json").write_text('{"top_k":{}}\n', encoding="utf-8")
            (ranking / "baseline_comparison.json").write_text('{"publication":{}}\n', encoding="utf-8")

            result = freeze_evaluation(
                label="2026-03-18-r1",
                validation_source_dir=validation,
                ranking_source_dir=ranking,
                output_root=output_root,
            )

            self.assertTrue((result.validation_dir / "validation_link_publication.jsonl").exists())
            self.assertTrue((result.validation_dir / "validation_link_locus.jsonl").exists())
            self.assertTrue((result.validation_dir / "holdout_summary.json").exists())

            self.assertTrue((result.ranking_dir / "ranking_publication_report.json").exists())
            self.assertTrue((result.ranking_dir / "ranking_locus_report.json").exists())
            self.assertTrue((result.ranking_dir / "baseline_comparison.json").exists())

            payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["label"], "2026-03-18-r1")
            self.assertEqual(len(payload["artifacts"]), 6)

    def test_freeze_evaluation_rejects_existing_label(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            validation = root / "validation"
            ranking = root / "ranking"
            output_root = root / "processed"
            validation.mkdir(parents=True, exist_ok=True)
            ranking.mkdir(parents=True, exist_ok=True)

            (validation / "validation_link_publication.jsonl").write_text('{"x":1}\n', encoding="utf-8")
            (validation / "validation_link_locus.jsonl").write_text('{"x":2}\n', encoding="utf-8")
            (validation / "holdout_summary.json").write_text('{"ok":true}\n', encoding="utf-8")

            (ranking / "ranking_publication_report.json").write_text('{"top_k":{}}\n', encoding="utf-8")
            (ranking / "ranking_locus_report.json").write_text('{"top_k":{}}\n', encoding="utf-8")
            (ranking / "baseline_comparison.json").write_text('{"publication":{}}\n', encoding="utf-8")

            freeze_evaluation(
                label="repeatable-label",
                validation_source_dir=validation,
                ranking_source_dir=ranking,
                output_root=output_root,
            )

            with self.assertRaises(FileExistsError):
                freeze_evaluation(
                    label="repeatable-label",
                    validation_source_dir=validation,
                    ranking_source_dir=ranking,
                    output_root=output_root,
                )


if __name__ == "__main__":
    unittest.main()
