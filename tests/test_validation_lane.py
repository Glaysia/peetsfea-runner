from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from peetsfea_runner import build_enroot_validation_lane_config


class TestValidationLane(unittest.TestCase):
    def test_build_enroot_validation_lane_config_for_prune_creates_input_queue_only_canary_layout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "examples").mkdir(parents=True, exist_ok=True)
            (repo_root / "examples" / "sample_0318.aedt").write_text("sample", encoding="utf-8")
            (repo_root / ".ssh").mkdir(parents=True, exist_ok=True)
            (repo_root / ".ssh" / "config").write_text("Host gate1-harry261\n", encoding="utf-8")

            config = build_enroot_validation_lane_config(
                repo_root=repo_root,
                lane="prune",
                window="00",
                remote_container_image="~/runtime/enroot/aedt.sqsh",
            )

            root = repo_root / "tmp" / "tonight-canary" / "00" / "prune"
            self.assertEqual(config.input_queue_dir, str(root / "input"))
            self.assertEqual(config.output_root_dir, str(root / "output"))
            self.assertEqual(config.delete_failed_quarantine_dir, str(root / "delete_failed"))
            self.assertEqual(config.metadata_db_path, str(root / "state.duckdb"))
            self.assertTrue((root / "input" / "sample.aedt").is_file())
            self.assertTrue((root / "input" / "sample.aedt.ready").is_file())
            self.assertEqual(config.input_source_policy, "input_queue_only")
            self.assertFalse(config.continuous_mode)
            self.assertEqual(config.remote_container_runtime, "enroot")
            self.assertEqual(config.ssh_config_path, str(repo_root / ".ssh" / "config"))
            self.assertEqual(config.host, "gate1-harry261")
            self.assertEqual(config.remote_root, "~/aedt_runs")
            self.assertEqual(config.slots_per_job, 48)
            self.assertEqual(config.cpus_per_job, 48)
            self.assertEqual(config.cores_per_slot, 4)
            self.assertEqual(config.tasks_per_slot, 1)
            self.assertEqual(len(config.accounts_registry), 1)

    def test_build_enroot_validation_lane_config_for_preserve_uses_single_account_shape(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "examples").mkdir(parents=True, exist_ok=True)
            (repo_root / "examples" / "sample_0318.aedt").write_text("sample", encoding="utf-8")

            config = build_enroot_validation_lane_config(
                repo_root=repo_root,
                lane="preserve",
                window="02",
                remote_container_image="~/runtime/enroot/aedt.sqsh",
            )

            self.assertEqual(config.run_namespace, "")
            self.assertEqual(config.host, "gate1-harry261")
            self.assertEqual(config.remote_root, "~/aedt_runs")
            self.assertEqual([account.host_alias for account in config.accounts_registry], ["gate1-harry261"])
            self.assertEqual(config.slots_per_job, 2)
            self.assertEqual(config.cpus_per_job, 32)
            self.assertEqual(config.cores_per_slot, 16)
            self.assertEqual(config.tasks_per_slot, 4)
            self.assertEqual(config.ssh_config_path, "")

    def test_build_enroot_validation_lane_config_rejects_invalid_lane(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir)
            (repo_root / "examples").mkdir(parents=True, exist_ok=True)
            (repo_root / "examples" / "sample_0318.aedt").write_text("sample", encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "lane must be 'prune' or 'preserve'"):
                build_enroot_validation_lane_config(
                    repo_root=repo_root,
                    lane="live",
                    window="00",
                    remote_container_image="~/runtime/enroot/aedt.sqsh",
                )


if __name__ == "__main__":
    unittest.main()
