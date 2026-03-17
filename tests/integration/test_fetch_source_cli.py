from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from panccre.cli.main import main


class FetchSourceCLITests(unittest.TestCase):
    def test_fetch_source_writes_manifest_and_lock(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source = tmp_path / "source.bed"
            source.write_text("chr20\t100\t200\tEH38E000001\n", encoding="utf-8")

            raw_root = tmp_path / "raw"
            manifest_root = tmp_path / "manifests"
            lock_file = tmp_path / "manifest.lock.json"

            exit_code = main(
                [
                    "fetch-source",
                    "--download-url",
                    f"file://{source}",
                    "--source-id",
                    "encode_ccre_fixture",
                    "--version",
                    "2026-03",
                    "--license",
                    "public",
                    "--genome-build",
                    "GRCh38",
                    "--parser-version",
                    "0.1.0",
                    "--format",
                    "bed",
                    "--raw-root",
                    str(raw_root),
                    "--manifest-root",
                    str(manifest_root),
                    "--lock-file",
                    str(lock_file),
                ]
            )

            self.assertEqual(exit_code, 0)

            manifest_path = manifest_root / "encode_ccre_fixture" / "2026-03.json"
            self.assertTrue(manifest_path.exists())
            self.assertTrue(lock_file.exists())

            lock = json.loads(lock_file.read_text(encoding="utf-8"))
            self.assertIn("encode_ccre_fixture@2026-03", lock["entries"])


if __name__ == "__main__":
    unittest.main()
