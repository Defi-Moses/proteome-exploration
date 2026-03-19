from __future__ import annotations

from pathlib import Path
import subprocess
import tempfile
import unittest

import yaml


FIXTURE_BED = Path(__file__).resolve().parents[1] / "golden" / "chr20" / "encode_ccre_chr20_fixture.bed"


class ReleaseScriptsIntegrationTests(unittest.TestCase):
    def test_release_build_and_contract_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            output_root = Path(tmpdir) / "releases"
            label = "it-release"

            build = subprocess.run(
                [
                    "python3",
                    "scripts/release_phase1.py",
                    "--label",
                    label,
                    "--output-root",
                    str(output_root),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(build.returncode, 0, msg=f"build failed\nstdout={build.stdout}\nstderr={build.stderr}")

            manifest_path = output_root / label / "release_manifest.json"
            self.assertTrue(manifest_path.exists())

            check = subprocess.run(
                [
                    "python3",
                    "scripts/check_release_contract.py",
                    "--release-manifest",
                    str(manifest_path),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(check.returncode, 0, msg=f"check failed\nstdout={check.stdout}\nstderr={check.stderr}")

    def test_bootstrap_real_data_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            raw_root = tmp / "raw"
            manifest_root = tmp / "manifests"
            lock_file = manifest_root / "manifest.lock.json"
            config_path = tmp / "sources.yaml"

            config_payload = {
                "sources": [
                    {
                        "source_id": "fixture_source",
                        "name": "Fixture BED",
                        "version": "2026-03",
                        "download_url": f"file://{FIXTURE_BED.resolve()}",
                        "license": "public",
                        "genome_build": "GRCh38",
                        "parser_version": "0.1.0",
                        "format": "bed",
                        "notes": "integration test fixture",
                    },
                    {
                        "source_id": "disabled_placeholder_source",
                        "version": "2026-03",
                        "download_url": "<manual-mirror-url>",
                        "license": "public",
                        "genome_build": "GRCh38",
                        "parser_version": "0.1.0",
                        "enabled": False,
                    },
                ]
            }
            config_path.write_text(yaml.safe_dump(config_payload, sort_keys=False), encoding="utf-8")

            dry_run = subprocess.run(
                [
                    "python3",
                    "scripts/bootstrap_real_data.py",
                    "--config",
                    str(config_path),
                    "--raw-root",
                    str(raw_root),
                    "--manifest-root",
                    str(manifest_root),
                    "--lock-file",
                    str(lock_file),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(dry_run.returncode, 0, msg=f"dry-run failed\nstdout={dry_run.stdout}\nstderr={dry_run.stderr}")
            self.assertIn("dry_run source=fixture_source@2026-03", dry_run.stdout)
            self.assertIn("skipped source=disabled_placeholder_source@2026-03 reason=disabled", dry_run.stdout)

            execute = subprocess.run(
                [
                    "python3",
                    "scripts/bootstrap_real_data.py",
                    "--config",
                    str(config_path),
                    "--raw-root",
                    str(raw_root),
                    "--manifest-root",
                    str(manifest_root),
                    "--lock-file",
                    str(lock_file),
                    "--execute",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(execute.returncode, 0, msg=f"execute failed\nstdout={execute.stdout}\nstderr={execute.stderr}")

            self.assertTrue((raw_root / "fixture_source" / "2026-03").exists())
            self.assertTrue((manifest_root / "fixture_source" / "2026-03.json").exists())
            self.assertTrue(lock_file.exists())
            self.assertTrue((manifest_root / "bootstrap_summary.json").exists())


if __name__ == "__main__":
    unittest.main()
