from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from panccre.manifests import (
    build_manifest_entry,
    fetch_source_artifact,
    load_manifest_lock,
    manifest_lock_key,
    upsert_manifest_lock_entry,
)


class ManifestDownloadLockTests(unittest.TestCase):
    def test_fetch_and_lock_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source = tmp_path / "source.tsv"
            source.write_text("chr20\t100\t200\n", encoding="utf-8")

            raw_root = tmp_path / "raw"
            lock_path = tmp_path / "manifest.lock.json"
            download = fetch_source_artifact(
                download_url=f"file://{source}",
                raw_root=raw_root,
                source_id="encode_ccre_fixture",
                version="2026-03",
            )

            self.assertTrue(download.artifact_path.exists())
            self.assertFalse(download.reused_existing)

            manifest = build_manifest_entry(
                file_path=download.artifact_path,
                source_id="encode_ccre_fixture",
                name="Fixture",
                version="2026-03",
                download_url=f"file://{source}",
                download_date="2026-03-17",
                license_name="public",
                genome_build="GRCh38",
                parser_version="0.1.0",
                file_format="bed",
            )

            upsert_manifest_lock_entry(
                lock_path=lock_path,
                manifest=manifest,
                artifact_path=download.artifact_path,
            )

            lock = load_manifest_lock(lock_path)
            key = manifest_lock_key(manifest)
            self.assertIn(key, lock["entries"])
            self.assertEqual(lock["entries"][key]["checksum"], manifest.checksum)

            reused = fetch_source_artifact(
                download_url=f"file://{source}",
                raw_root=raw_root,
                source_id="encode_ccre_fixture",
                version="2026-03",
            )
            self.assertTrue(reused.reused_existing)

    def test_lock_rejects_checksum_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            source = tmp_path / "source.tsv"
            source.write_text("chr20\t100\t200\n", encoding="utf-8")

            raw_root = tmp_path / "raw"
            lock_path = tmp_path / "manifest.lock.json"
            download = fetch_source_artifact(
                download_url=f"file://{source}",
                raw_root=raw_root,
                source_id="encode_ccre_fixture",
                version="2026-03",
            )

            manifest = build_manifest_entry(
                file_path=download.artifact_path,
                source_id="encode_ccre_fixture",
                name="Fixture",
                version="2026-03",
                download_url=f"file://{source}",
                download_date="2026-03-17",
                license_name="public",
                genome_build="GRCh38",
                parser_version="0.1.0",
            )

            upsert_manifest_lock_entry(
                lock_path=lock_path,
                manifest=manifest,
                artifact_path=download.artifact_path,
            )

            download.artifact_path.write_text("mutated\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                upsert_manifest_lock_entry(
                    lock_path=lock_path,
                    manifest=manifest,
                    artifact_path=download.artifact_path,
                )


if __name__ == "__main__":
    unittest.main()
