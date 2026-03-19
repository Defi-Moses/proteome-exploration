from __future__ import annotations

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import panccre.worker.main as worker_main


class WorkerRegistryPublishTests(unittest.TestCase):
    def test_publish_local_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            worker_main, "_publish_registry_atomically", return_value=None
        ) as local_publish, patch.object(worker_main, "_publish_registry_via_api", return_value=None) as api_publish, patch.dict(
            os.environ, {"PANCCRE_REGISTRY_PUBLISH_MODE": "local"}, clear=False
        ):
            worker_main._publish_registry(
                source_registry_dir=Path(tmpdir) / "source",
                target_registry_dir=Path(tmpdir) / "target",
                run_tag="run-local",
            )
            local_publish.assert_called_once()
            api_publish.assert_not_called()

    def test_publish_api_sync_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            worker_main, "_publish_registry_atomically", return_value=None
        ) as local_publish, patch.object(worker_main, "_publish_registry_via_api", return_value=None) as api_publish, patch.dict(
            os.environ, {"PANCCRE_REGISTRY_PUBLISH_MODE": "api_sync"}, clear=False
        ):
            worker_main._publish_registry(
                source_registry_dir=Path(tmpdir) / "source",
                target_registry_dir=Path(tmpdir) / "target",
                run_tag="run-api",
            )
            local_publish.assert_not_called()
            api_publish.assert_called_once()

    def test_publish_dual_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, patch.object(
            worker_main, "_publish_registry_atomically", return_value=None
        ) as local_publish, patch.object(worker_main, "_publish_registry_via_api", return_value=None) as api_publish, patch.dict(
            os.environ, {"PANCCRE_REGISTRY_PUBLISH_MODE": "dual"}, clear=False
        ):
            worker_main._publish_registry(
                source_registry_dir=Path(tmpdir) / "source",
                target_registry_dir=Path(tmpdir) / "target",
                run_tag="run-dual",
            )
            local_publish.assert_called_once()
            api_publish.assert_called_once()

    def test_publish_invalid_mode_raises(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir, patch.dict(
            os.environ, {"PANCCRE_REGISTRY_PUBLISH_MODE": "wat"}, clear=False
        ):
            with self.assertRaisesRegex(ValueError, "PANCCRE_REGISTRY_PUBLISH_MODE"):
                worker_main._publish_registry(
                    source_registry_dir=Path(tmpdir) / "source",
                    target_registry_dir=Path(tmpdir) / "target",
                    run_tag="run-bad",
                )

    def test_default_api_sync_url_uses_linked_domain(self) -> None:
        with patch.dict(
            os.environ,
            {
                "PANCCRE_API_SYNC_URL": "",
                "RAILWAY_SERVICE__PANCCRE_API_URL": "panccreapi-production.up.railway.app",
            },
            clear=False,
        ):
            self.assertEqual(
                worker_main._default_api_sync_url(),
                "https://panccreapi-production.up.railway.app/internal/registry/sync",
            )


if __name__ == "__main__":
    unittest.main()
