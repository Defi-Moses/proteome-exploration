from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from panccre.manifests.builder import build_manifest_entry, compute_sha256, load_manifest_file, write_manifest_file
from panccre.manifests.schema import ManifestValidationError, manifest_from_dict, validate_manifest_dict


class ManifestBuilderTests(unittest.TestCase):
    def test_build_manifest_entry_writes_and_roundtrips(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source_file = tmp_path / "source.tsv"
            source_file.write_text("chr20\t100\t200\n", encoding="utf-8")

            manifest = build_manifest_entry(
                file_path=source_file,
                source_id="encode_ccre_v4",
                name="ENCODE cCRE registry",
                version="2026-01",
                download_url="https://example.org/encode_ccre_v4.bed",
                download_date="2026-03-17",
                license_name="public",
                genome_build="GRCh38",
                parser_version="0.1.0",
                file_format="bed",
                notes="fixture",
            )

            self.assertEqual(manifest.checksum, compute_sha256(source_file))
            self.assertEqual(manifest.source_id, "encode_ccre_v4")

            output_manifest = tmp_path / "manifest.json"
            write_manifest_file(manifest, output_manifest)

            loaded = load_manifest_file(output_manifest)
            self.assertEqual(loaded, manifest)

    def test_validate_manifest_dict_reports_missing_required_fields(self) -> None:
        errors = validate_manifest_dict({"source_id": "encode_ccre_v4"})
        self.assertTrue(errors)
        self.assertTrue(any("Missing required field" in error for error in errors))

    def test_manifest_from_dict_rejects_invalid_checksum(self) -> None:
        payload = {
            "source_id": "encode_ccre_v4",
            "version": "2026-01",
            "download_url": "https://example.org/x.bed",
            "download_date": "2026-03-17",
            "checksum": "invalid",
            "license": "public",
            "genome_build": "GRCh38",
            "parser_version": "0.1.0",
        }

        with self.assertRaises(ManifestValidationError):
            manifest_from_dict(payload)

    def test_load_manifest_requires_object_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps([1, 2, 3]), encoding="utf-8")

            with self.assertRaises(ValueError):
                load_manifest_file(path)


if __name__ == "__main__":
    unittest.main()
