from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import panccre.worker.main as worker_main


class WorkerProjectionModeTests(unittest.TestCase):
    def _run_pipeline_with_capture(self, env_overrides: dict[str, str]) -> list[list[str]]:
        commands: list[list[str]] = []

        def _capture_command(args: list[str], *, env: dict[str, str]) -> None:
            del env
            commands.append(list(args))

        merged_env = {"PANCCRE_REGISTRY_PUBLISH_MODE": "local"}
        merged_env.update(env_overrides)

        with patch.object(worker_main, "_run_command", side_effect=_capture_command), patch.object(
            worker_main, "_validate_registry_dir", return_value=None
        ), patch.object(worker_main, "_publish_registry_atomically", return_value=None), patch.dict(
            os.environ, merged_env, clear=False
        ):
            exit_code = worker_main._run_pipeline_once()
            self.assertEqual(exit_code, 0)
        return commands

    def test_pipeline_defaults_to_fixture_projection(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            commands = self._run_pipeline_with_capture(
                {
                    "PANCCRE_PIPELINE_OUTPUT_ROOT": str(tmp_path / "runs"),
                    "PANCCRE_PUBLISH_REGISTRY_DIR": str(tmp_path / "registry"),
                    "PANCCRE_PIPELINE_RUN_TAG": "fixture-mode-test",
                    "PANCCRE_FREEZE_EVALUATION": "0",
                    "PANCCRE_BUILD_REPORT_BUNDLE": "0",
                }
            )

            projection_commands = [cmd for cmd in commands if len(cmd) >= 3 and cmd[2].startswith("project-")]
            self.assertEqual(len(projection_commands), 1)
            self.assertEqual(projection_commands[0][2], "project-fixture")

    def test_pipeline_uses_vcf_projection_when_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            variants_path = tmp_path / "variants.vcf.gz"
            commands = self._run_pipeline_with_capture(
                {
                    "PANCCRE_PIPELINE_OUTPUT_ROOT": str(tmp_path / "runs"),
                    "PANCCRE_PUBLISH_REGISTRY_DIR": str(tmp_path / "registry"),
                    "PANCCRE_PIPELINE_RUN_TAG": "vcf-mode-test",
                    "PANCCRE_FREEZE_EVALUATION": "0",
                    "PANCCRE_BUILD_REPORT_BUNDLE": "0",
                    "PANCCRE_PIPELINE_PROJECTION_MODE": "vcf",
                    "PANCCRE_PIPELINE_VARIANTS": str(variants_path),
                    "PANCCRE_PIPELINE_HAPLOTYPES": str(tmp_path / "haps.tsv"),
                    "PANCCRE_PIPELINE_MAX_VARIANTS": "2500",
                }
            )

            projection_commands = [cmd for cmd in commands if len(cmd) >= 3 and cmd[2].startswith("project-")]
            self.assertEqual(len(projection_commands), 1)
            command = projection_commands[0]
            self.assertEqual(command[2], "project-vcf")
            self.assertIn("--variants", command)
            self.assertIn(str(variants_path), command)
            self.assertIn("--haplotypes", command)
            self.assertIn("--max-variants", command)

    def test_pipeline_uses_real_ingest_when_ccre_bed_configured(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            ccre_bed = tmp_path / "ccre_real.bed"
            assay = tmp_path / "assay.tsv"
            commands = self._run_pipeline_with_capture(
                {
                    "PANCCRE_PIPELINE_OUTPUT_ROOT": str(tmp_path / "runs"),
                    "PANCCRE_PUBLISH_REGISTRY_DIR": str(tmp_path / "registry"),
                    "PANCCRE_PIPELINE_RUN_TAG": "real-ingest-test",
                    "PANCCRE_FREEZE_EVALUATION": "0",
                    "PANCCRE_BUILD_REPORT_BUNDLE": "0",
                    "PANCCRE_PIPELINE_CCRE_BED": str(ccre_bed),
                    "PANCCRE_PIPELINE_SOURCE_RELEASE": "encode-v4-2026-01",
                    "PANCCRE_PIPELINE_ASSAY_SOURCE": str(assay),
                    "PANCCRE_PIPELINE_ASSAY_SOURCE_FORMAT": "csv",
                }
            )

            ingest_commands = [cmd for cmd in commands if len(cmd) >= 3 and cmd[2] in {"smoke-ingest", "ingest-ccre"}]
            self.assertEqual(len(ingest_commands), 1)
            ingest = ingest_commands[0]
            self.assertEqual(ingest[2], "ingest-ccre")
            self.assertIn("--input-bed", ingest)
            self.assertIn(str(ccre_bed), ingest)
            self.assertIn("--source-release", ingest)
            self.assertIn("encode-v4-2026-01", ingest)

            validation_commands = [cmd for cmd in commands if len(cmd) >= 3 and cmd[2] == "build-validation-link"]
            self.assertEqual(len(validation_commands), 1)
            validation = validation_commands[0]
            self.assertIn("--assay-source", validation)
            self.assertIn(str(assay), validation)
            self.assertIn("--assay-source-format", validation)

    def test_vcf_projection_requires_variants_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(
            os.environ,
            {
                "PANCCRE_PIPELINE_OUTPUT_ROOT": str(Path(tmpdir) / "runs"),
                "PANCCRE_PUBLISH_REGISTRY_DIR": str(Path(tmpdir) / "registry"),
                "PANCCRE_PIPELINE_RUN_TAG": "vcf-mode-missing-variants",
                "PANCCRE_FREEZE_EVALUATION": "0",
                "PANCCRE_BUILD_REPORT_BUNDLE": "0",
                "PANCCRE_PIPELINE_PROJECTION_MODE": "vcf",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(ValueError, "PANCCRE_PIPELINE_VARIANTS must be set"):
                worker_main._run_pipeline_once()


if __name__ == "__main__":
    unittest.main()
